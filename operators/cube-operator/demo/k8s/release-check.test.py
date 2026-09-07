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
                                    evidence=self.evidence_file, from_lock=None, transition='upgrade',
                                    from_lock_sha256=None, from_root=None, edges=None, edges_sha256=None)

    def write(self, name, obj):
        path = self.root / name
        path.write_text(json.dumps(obj))
        return path

    def add_report(self, scenario, **extra):
        value = {'release_id': self.lock['release_id'], 'scenario': scenario,
                 'case': 'isolated fixture only: ' + scenario, 'status': 'PASS',
                 'observed_images': self.lock['images'], 'started_at': '2026-09-07T00:00:00Z',
                 'finished_at': '2026-09-07T00:00:01Z', 'assertions': [{'name': 'rows', 'expected': [1, 2], 'actual': [1, 2]}],
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
        self.transition_fixture()
        self.args.transition = 'rollback'
        with self.assertRaisesRegex(ValueError, 'directed approved edge absent'):
            release.check(self.args)

    def test_upgrade_requires_partial_failure_and_interruption(self):
        self.transition_fixture()
        del self.evidence['checks']['interruption']
        self.approve_fixture()
        with self.assertRaisesRegex(ValueError, 'interruption'):
            release.check(self.args)

    def transition_fixture(self, mode='mixed-version'):
        # Synthetic fixtures test gate mechanics; never published as real edges.
        self.old = copy.deepcopy(self.lock)
        self.old.pop('release_id')
        self.old['images'] = {role: 'fixture/old@sha256:' + 'b' * 64 for role in release.ROLES}
        self.old['cluster']['spec']['images'] = {role: self.old['images'][role]
                                               for role in self.cluster['spec']['images']}
        self.old['release_id'] = release.sha(release.encoded(self.old))
        self.args.from_lock = self.write('old-lock.json', self.old)
        self.args.from_lock_sha256 = release.sha(self.args.from_lock.read_bytes())
        self.args.from_root = self.root
        self.evidence['installation'] = 'existing'
        pair = {'from_release_id': self.old['release_id'], 'to_release_id': self.lock['release_id']}
        mixed = {**self.old['images'], 'metaStore': self.lock['images']['metaStore']}
        self.add_report('upgrade', **pair, mode=mode, old_observed_images=self.old['images'],
                        tested_image_sets=[self.old['images'], mixed, self.lock['images']])
        for scenario in ('interruption', 'partial-failure', 'old-worker-drained', 'persistent-state-readable',
                         'mixed-version-rpc' if mode == 'mixed-version' else 'maintenance-fencing'):
            self.add_report(scenario, **pair)
        self.edge = {**pair, 'transition': 'upgrade', 'mode': mode,
                     'source_lock_sha256': self.args.from_lock_sha256,
                     'target_lock_sha256': self.args.lock_sha256}
        self.approve_fixture()

    def approve_fixture(self):
        self.sync()
        self.edge['evidence_sha256'] = release.sha(self.args.evidence.read_bytes())
        self.args.edges = self.write('edges.json', {'schema': 1, 'edges': [self.edge]})
        self.args.edges_sha256 = release.sha(self.args.edges.read_bytes())

    def replace_transition(self, **changes):
        value = release.load(self.root / 'upgrade.json')
        value.update(changes)
        self.write('upgrade.json', value)
        self.evidence['checks']['upgrade'] = release.artifact(self.root, 'upgrade.json')
        self.approve_fixture()

    def test_accepts_explicit_approved_fixture_edge(self):
        self.transition_fixture()
        self.assertEqual(release.check(self.args)['status'], 'EVIDENCE_CONTRACT_SATISFIED')

    def test_accepts_fenced_maintenance_fixture_with_own_evidence(self):
        self.transition_fixture('fenced-maintenance')
        self.replace_transition(tested_image_sets=[self.old['images'], self.lock['images']])
        self.assertEqual(release.check(self.args)['scope'], 'acceptance')

    def test_rejects_missing_independent_edge_approval(self):
        self.transition_fixture()
        self.args.edges_sha256 = None
        with self.assertRaisesRegex(ValueError, 'independently approved --edges-sha256'):
            release.check(self.args)

    def test_rejects_changed_approved_registry(self):
        self.transition_fixture()
        self.write('edges.json', {'schema': 1, 'edges': []})
        with self.assertRaisesRegex(ValueError, 'registry approval hash'):
            release.check(self.args)

    def test_rejects_replaced_evidence_index_after_approval(self):
        self.transition_fixture()
        self.evidence['unapproved_change'] = True
        self.sync()
        with self.assertRaisesRegex(ValueError, 'evidence hash differs'):
            release.check(self.args)

    def test_rejects_missing_old_lock_approval(self):
        self.transition_fixture()
        self.args.from_lock_sha256 = None
        with self.assertRaisesRegex(ValueError, 'old lock independently approved'):
            release.check(self.args)

    def test_rejects_missing_old_artifacts(self):
        self.transition_fixture()
        self.args.from_root = self.root / 'missing-archive'
        with self.assertRaisesRegex(ValueError, 'artifact missing'):
            release.check(self.args)

    def test_rejects_wrong_old_runtime_images(self):
        self.transition_fixture()
        self.replace_transition(old_observed_images=self.lock['images'])
        with self.assertRaisesRegex(ValueError, 'old runtime image'):
            release.check(self.args)

    def test_rejects_mixed_version_claim_without_mixed_observation(self):
        self.transition_fixture()
        self.replace_transition(tested_image_sets=[self.old['images'], self.lock['images']])
        with self.assertRaisesRegex(ValueError, 'lacks a mixed runtime'):
            release.check(self.args)

    def test_rejects_unapproved_third_version(self):
        self.transition_fixture()
        third = {**self.old['images'], 'api': 'fixture/other@sha256:' + 'c' * 64}
        self.replace_transition(tested_image_sets=[self.old['images'], third, self.lock['images']])
        with self.assertRaisesRegex(ValueError, 'outside the approved pair'):
            release.check(self.args)

    def test_rejects_boolean_as_numeric_assertion(self):
        self.add_report('historical-1', assertions=[{'name': 'publication count', 'expected': 1, 'actual': True}])
        self.sync()
        with self.assertRaisesRegex(ValueError, 'failed assertion'):
            release.check(self.args)

    def test_rejects_reversed_observation_times(self):
        self.add_report('historical-1', finished_at='2026-09-06T00:00:00Z')
        self.sync()
        with self.assertRaisesRegex(ValueError, 'finishes before'):
            release.check(self.args)

    def test_rejects_timezone_free_observation(self):
        self.add_report('historical-1', started_at='2026-09-07T00:00:00')
        self.sync()
        with self.assertRaisesRegex(ValueError, 'needs a timezone'):
            release.check(self.args)

    def test_rejects_blank_router_node_as_second_node(self):
        self.args.mode = 'production'
        self.evidence['production'] = {'router_nodes': ['real-node', '']}
        self.sync()
        with self.assertRaisesRegex(ValueError, 'multi-node'):
            release.check(self.args)

    def test_rejects_nonfinite_production_budget(self):
        self.args.mode = 'production'
        self.evidence['production'] = {'router_nodes': ['fixture-a', 'fixture-b'], 'metastore_writers': 1,
                                      'manager_namespace': 'fixture', 'lease_namespace': 'fixture',
                                      'rpo_seconds': 'HUGE'}
        self.sync()
        self.evidence_file.write_text(self.evidence_file.read_text().replace('"HUGE"', '1e999'))
        with self.assertRaisesRegex(ValueError, 'nonfinite rpo_seconds'):
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
