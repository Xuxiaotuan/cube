#!/usr/bin/env python3
"""Bounded, stdlib-only local fixtures. No live cluster and no production evidence."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

SCRIPT = Path(__file__).with_name('release-check.py')
spec = importlib.util.spec_from_file_location('release_check', SCRIPT)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.image = 'fixture/cube@sha256:' + 'a' * 64
        self.cluster = {'apiVersion': 'cubestore.io/v1alpha1', 'kind': 'CubeCluster',
                        'metadata': {'name': 'fixture', 'namespace': 'fixture'},
                        'spec': {'images': {k: self.image for k in
                                 ('api', 'router', 'metaStore', 'worker', 'leaseAgent')},
                                 'metaStore': {'replicas': 1}}}
        self.cluster_file = self.write('cluster.json', self.cluster)
        self.write('raw.json', {'rows': [1, 2], 'buildId': 'fixture-build', 'tableId': 'fixture-table'})
        self.lock_file = self.root / 'lock.json'
        args = SimpleNamespace(root=self.root, cluster=self.cluster_file, operator_image=self.image,
                               artifact=[c + '=raw.json' for c in ('crd', 'config', 'test', 'source')],
                               out=self.lock_file)
        release.freeze(args)
        self.lock = release.read_lock(self.lock_file)
        self.evidence = {'release_id': self.lock['release_id'], 'installation': 'fresh', 'checks': {}}
        for scenario in release.SUITE + release.PRODUCTION:
            self.add_report(scenario)
        self.evidence_file = self.write('evidence.json', self.evidence)
        self.args = SimpleNamespace(root=self.root, lock=self.lock_file,
                                    lock_sha256=release.sha(self.lock_file.read_bytes()), mode='acceptance',
                                    evidence=self.evidence_file, from_lock=None, transition='upgrade')

    def write(self, name, obj):
        path = self.root / name
        path.write_text(json.dumps(obj))
        return path

    def add_report(self, scenario, **extra):
        value = {'release_id': self.lock['release_id'], 'scenario': scenario,
                 'case': 'isolated fixture only: ' + scenario, 'status': 'PASS',
                 'observed_images': self.lock['images'], 'started_at': 'fixture-start',
                 'finished_at': 'fixture-end', 'assertions': [{'name': 'rows', 'expected': [1, 2], 'actual': [1, 2]}],
                 'raw_evidence': [release.artifact(self.root, 'raw.json')], **extra}
        self.write(scenario + '.json', value)
        self.evidence['checks'][scenario] = release.artifact(self.root, scenario + '.json')

    def sync(self):
        self.write('evidence.json', self.evidence)

    def test_accepts_complete_fixture_not_production_claim(self):
        self.assertEqual(release.check(self.args)['scope'], 'acceptance')

    def test_rejects_missing_scenario(self):
        del self.evidence['checks']['refresher-crash']
        self.sync()
        with self.assertRaisesRegex(ValueError, 'refresher-crash'):
            release.check(self.args)

    def test_rejects_wrong_runtime_digest(self):
        self.add_report('historical-1', observed_images={})
        self.sync()
        with self.assertRaisesRegex(ValueError, 'runtime image'):
            release.check(self.args)

    def test_rejects_failed_data_even_with_pass_status(self):
        self.add_report('historical-1', assertions=[{'name': 'rows', 'expected': [1, 2], 'actual': [1]}])
        self.sync()
        with self.assertRaisesRegex(ValueError, 'failed assertion'):
            release.check(self.args)

    def test_rejects_stale_release_report(self):
        self.add_report('historical-1', release_id='old')
        self.sync()
        with self.assertRaisesRegex(ValueError, 'wrong release'):
            release.check(self.args)

    def test_rejects_modified_frozen_file(self):
        self.write('raw.json', {'changed': True})
        with self.assertRaisesRegex(ValueError, 'artifact changed'):
            release.check(self.args)

    def test_rejects_absent_raw_evidence(self):
        self.add_report('historical-1', raw_evidence=[])
        self.sync()
        with self.assertRaisesRegex(ValueError, 'no raw evidence'):
            release.check(self.args)

    def test_rejects_mutable_or_mixed_rust_images(self):
        for role, value in [('api', 'fixture:latest'), ('worker', 'fixture/cube@sha256:' + 'b' * 64)]:
            cluster = copy.deepcopy(self.cluster)
            cluster['spec']['images'][role] = value
            with self.assertRaises(ValueError):
                release.images(cluster, self.image)

    def test_rejects_path_escape(self):
        with self.assertRaisesRegex(ValueError, 'within root'):
            release.artifact(self.root, '../secret')

    def test_rejects_wrong_approved_hash(self):
        self.args.lock_sha256 = 'wrong'
        with self.assertRaisesRegex(ValueError, 'independently approved'):
            release.check(self.args)

    def test_existing_installation_needs_transition(self):
        self.evidence['installation'] = 'existing'
        self.sync()
        with self.assertRaisesRegex(ValueError, 'from-lock'):
            release.check(self.args)

    def test_rollback_requires_explicit_reverse_evidence(self):
        self.args.from_lock = self.lock_file
        self.args.transition = 'rollback'
        with self.assertRaisesRegex(ValueError, 'rollback'):
            release.check(self.args)

    def test_upgrade_requires_partial_failure_and_interruption(self):
        self.args.from_lock = self.lock_file
        self.add_report('upgrade', from_release_id=self.lock['release_id'],
                        to_release_id=self.lock['release_id'], mode='mixed-version')
        self.sync()
        with self.assertRaisesRegex(ValueError, 'interruption'):
            release.check(self.args)

    def test_production_blocks_single_node(self):
        self.args.mode = 'production'
        self.evidence['production'] = {'router_nodes': ['orbstack', 'orbstack']}
        self.sync()
        with self.assertRaisesRegex(ValueError, 'multi-node'):
            release.check(self.args)

    def test_production_requires_explicit_rpo_rto(self):
        self.args.mode = 'production'
        self.evidence['production'] = {'router_nodes': ['fixture-a', 'fixture-b'],
                                       'metastore_writers': 1, 'manager_namespace': 'fixture',
                                       'lease_namespace': 'fixture'}
        self.sync()
        with self.assertRaisesRegex(ValueError, 'rpo_seconds'):
            release.check(self.args)

    def test_cli_is_bounded_and_fail_closed(self):
        result = subprocess.run([sys.executable, str(SCRIPT), 'check', '--root', str(self.root),
                                 '--lock', str(self.lock_file), '--lock-sha256', 'wrong'],
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stderr)['status'], 'BLOCKED')


if __name__ == '__main__':
    unittest.main()
