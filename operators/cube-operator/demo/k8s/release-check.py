#!/usr/bin/env python3
"""Offline release freeze/evidence gate. Never invokes kubectl, shell, or networks."""
import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import sys

ROLES = {'operator', 'leaseAgent', 'router', 'metaStore', 'worker', 'api', 'refresher'}
# Historical IDs must be mapped to actual archived scenario names in each report.
SUITE = [*(f'historical-{i}' for i in range(1, 7)), 'control-plane',
         'worker-stale-attempt', 'refresher-crash', 'network-partition', 'repeated-load']
PRODUCTION = ['node-fencing', 'volume-reattach', 'backup-restore', 'object-reconciliation',
              'unfinished-build-restore', 'manager-rbac', 'drain-probes', 'admin-isolation']
DIGEST = re.compile(r'[^\s@]+@sha256:[0-9a-f]{64}\Z')


def timestamp(value):
    require(isinstance(value, str), 'observation timestamp must be an ISO8601 string')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.utcoffset() is not None, 'observation timestamp needs a timezone')
    return parsed


def require(ok, message):
    if not ok:
        raise ValueError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON key: ' + key)
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def local(root, name):
    require(isinstance(name, str) and bool(name), 'artifact path missing')
    path = (root / name).resolve()
    require(path.is_relative_to(root.resolve()) and not Path(name).is_absolute(),
            'artifact must stay within root: ' + name)
    require(path.is_file(), 'artifact missing: ' + name)
    return path


def artifact(root, name):
    return {'path': name, 'sha256': sha(local(root, name).read_bytes())}


def verify_artifact(root, ref):
    require(artifact(root, ref['path']) == ref, 'artifact changed: ' + ref['path'])
    return local(root, ref['path'])


def images(cluster, operator):
    spec = cluster['spec']
    result = dict(spec['images'])
    result['operator'] = operator
    result['refresher'] = result['api']  # Refresher has no separate image field in this API.
    require(set(result) == ROLES, 'unexpected or absent CubeCluster image role')
    require(all(isinstance(v, str) and DIGEST.fullmatch(v) for v in result.values()),
            'all images must be immutable repository@sha256:... references')
    require(len({result[r].split('@')[1] for r in ('router', 'metaStore', 'worker')}) == 1,
            'mixed Rust digests are not an approved artifact set')
    require(spec.get('metaStore', {}).get('replicas', 1) == 1, 'MetaStore must be singleton')
    require(spec.get('refresher', {}).get('replicas', 1) == 1, 'Refresher must be singleton')
    return result


def read_lock(path):
    lock = load(path)
    release_id = lock.pop('release_id')
    require(lock.get('schema') == 1 and sha(encoded(lock)) == release_id, 'invalid release content ID')
    lock['release_id'] = release_id
    require(lock['images'] == images(lock['cluster'], lock['images']['operator']), 'lock image mismatch')
    return lock


def verify_inventory(root, lock):
    require(lock['suite'] == SUITE, 'frozen suite differs from checker suite')
    for category in ('crd', 'config', 'test', 'source'):
        require(bool(lock['artifacts'][category]), 'empty artifact category: ' + category)
        for ref in lock['artifacts'][category]:
            verify_artifact(root, ref)


def approved_edge(args, old, lock, evidence):
    # Approval is the independently supplied SHA, not a self-declared protocol
    # label, "verified" boolean, release version or equal image digest.
    require(args.edges is not None and args.edges_sha256,
            'compatibility_blocked: --edges and independently approved --edges-sha256 required')
    require(sha(args.edges.read_bytes()) == args.edges_sha256, 'edge registry approval hash mismatch')
    registry = load(args.edges)
    require(registry.get('schema') == 1 and isinstance(registry.get('edges'), list), 'invalid edge registry')
    matches = [edge for edge in registry['edges']
               if edge.get('from_release_id') == old['release_id']
               and edge.get('to_release_id') == lock['release_id']
               and edge.get('transition') == args.transition]
    require(len(matches) == 1, 'compatibility_blocked: exact unique directed approved edge absent')
    edge = matches[0]
    require(edge.get('source_lock_sha256') == args.from_lock_sha256 and
            edge.get('target_lock_sha256') == args.lock_sha256, 'approved edge lock hashes differ')
    require(edge.get('evidence_sha256') == sha(args.evidence.read_bytes()), 'approved edge evidence hash differs')
    require(edge.get('mode') in ('mixed-version', 'fenced-maintenance'), 'approved edge mode missing')
    require(evidence.get('installation') == 'existing', 'transition evidence must describe an existing installation')
    return edge


def freeze(args):
    cluster = load(args.cluster)
    require(cluster.get('kind') == 'CubeCluster' and cluster.get('apiVersion') == 'cubestore.io/v1alpha1',
            'expected actual CubeCluster v1alpha1 JSON (not a new CRD contract)')
    # Exclude status/resourceVersion, retain all intended metadata and spec configuration.
    cluster = {k: cluster[k] for k in ('apiVersion', 'kind', 'metadata', 'spec')}
    cluster['metadata'] = {k: v for k, v in cluster['metadata'].items()
                           if k in ('name', 'namespace', 'labels', 'annotations')}
    groups = {'crd': [], 'config': [], 'test': [], 'source': []}
    for item in args.artifact:
        category, name = item.split('=', 1)
        require(category in groups, 'artifact kind must be crd/config/test/source')
        groups[category].append(artifact(args.root, name))
    require(all(groups.values()), 'freeze requires CRD, configuration, test and source artifacts')
    lock = {'schema': 1, 'cluster': cluster, 'images': images(cluster, args.operator_image),
            'artifacts': groups, 'suite': SUITE}
    lock['release_id'] = sha(encoded(lock))
    with args.out.open('x') as output:
        json.dump(lock, output, indent=2, sort_keys=True)
        output.write('\n')
    return {'status': 'FROZEN_NOT_ACCEPTED', 'release_id': lock['release_id'],
            'lock_sha256': sha(args.out.read_bytes())}


def report(root, ref, lock, scenario):
    value = load(verify_artifact(root, ref))
    require(value.get('release_id') == lock['release_id'], scenario + ': wrong release')
    require(value.get('scenario') == scenario and value.get('status') == 'PASS', scenario + ': not PASS')
    require(isinstance(value.get('case'), str) and value['case'].strip(), scenario + ': missing real case name')
    require(value.get('observed_images') == lock['images'], scenario + ': runtime image digest mismatch')
    require(timestamp(value.get('finished_at')) >= timestamp(value.get('started_at')),
            scenario + ': observation finishes before it starts')
    assertions = value.get('assertions')
    require(isinstance(assertions, list) and bool(assertions), scenario + ': no data assertions')
    for assertion in assertions:
        require(isinstance(assertion.get('name'), str) and assertion['name'].strip(), 'unnamed assertion')
        require('expected' in assertion and 'actual' in assertion and assertion['expected'] is not None,
                scenario + ': assertion has no expected/actual result')
        require(encoded(assertion['expected']) == encoded(assertion['actual']),
                scenario + ': failed assertion ' + assertion['name'])
    refs = value.get('raw_evidence', [])
    require(bool(refs), scenario + ': no raw evidence')
    for raw in refs:
        verify_artifact(root, raw)
    return value


def check(args):
    require(sha(args.lock.read_bytes()) == args.lock_sha256, 'lock hash differs from independently approved digest')
    lock = read_lock(args.lock)
    verify_inventory(args.root, lock)
    if args.mode == 'preflight':
        require(args.from_lock is None, 'transition requires acceptance evidence, not preflight')
        return {'status': 'ARTIFACTS_MATCH_NOT_COMPATIBILITY_PROOF', 'release_id': lock['release_id']}
    require(args.evidence is not None, 'acceptance requires evidence index')
    evidence = load(args.evidence)
    require(evidence.get('release_id') == lock['release_id'], 'evidence belongs to another release')
    checks = evidence.get('checks', {})
    required = SUITE + (PRODUCTION if args.mode == 'production' else [])
    for scenario in required:
        require(scenario in checks, 'evidence_incomplete: ' + scenario)
        report(args.evidence.parent, checks[scenario], lock, scenario)
    if args.from_lock:
        require(args.from_lock_sha256 and sha(args.from_lock.read_bytes()) == args.from_lock_sha256,
                'compatibility_blocked: old lock independently approved hash missing or mismatched')
        require(args.from_root is not None, 'compatibility_blocked: --from-root archived old artifacts required')
        old = read_lock(args.from_lock)
        verify_inventory(args.from_root, old)
        require(old['release_id'] != lock['release_id'], 'same release is not an upgrade or rollback edge')
        edge = approved_edge(args, old, lock, evidence)
        scenario = args.transition
        require(scenario in checks, 'missing exact transition evidence: ' + scenario)
        transition = report(args.evidence.parent, checks[scenario], lock, scenario)
        require(transition.get('from_release_id') == old['release_id'], 'wrong transition source')
        require(transition.get('to_release_id') == lock['release_id'], 'wrong transition target')
        require(transition.get('mode') == edge['mode'], 'transition differs from approved mode')
        require(transition.get('old_observed_images') == old['images'], 'old runtime image digest mismatch')
        tested = transition.get('tested_image_sets', [])
        require(isinstance(tested, list) and old['images'] in tested and lock['images'] in tested,
                'transition lacks source and target runtime observations')
        for combination in tested:
            require(isinstance(combination, dict) and set(combination) == ROLES,
                    'tested image set has missing or unknown roles')
            require(all(combination[r] in (old['images'][r], lock['images'][r]) for r in ROLES),
                    'tested image set contains an image outside the approved pair')
        mode_check = 'maintenance-fencing'
        if edge['mode'] == 'mixed-version':
            require(any(combo != old['images'] and combo != lock['images'] for combo in tested),
                    'mixed-version edge lacks a mixed runtime observation')
            mode_check = 'mixed-version-rpc'
        for name in ('interruption', 'partial-failure', 'old-worker-drained', 'persistent-state-readable', mode_check):
            require(name in checks, 'transition evidence missing: ' + name)
            detail = report(args.evidence.parent, checks[name], lock, name)
            require(detail.get('from_release_id') == old['release_id'] and
                    detail.get('to_release_id') == lock['release_id'], 'transition detail has wrong release pair')
    else:
        require(evidence.get('installation') == 'fresh', 'existing installation requires --from-lock')
    if args.mode == 'production':
        topology = evidence.get('production', {})
        nodes = topology.get('router_nodes', [])
        require(isinstance(nodes, list) and all(isinstance(n, str) and n.strip() for n in nodes)
                and len(set(nodes)) >= 2, 'external_blocked: multi-node proof missing')
        require(type(topology.get('metastore_writers')) is int and topology['metastore_writers'] == 1,
                'single writer not confirmed')
        require(topology.get('manager_namespace') == topology.get('lease_namespace') and
                bool(topology.get('manager_namespace')), 'Manager Lease namespace mismatch')
        for field in ('rpo_seconds', 'rto_seconds', 'observed_data_loss_seconds', 'observed_restore_seconds'):
            value = topology.get(field)
            require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
                    'external_blocked: missing or nonfinite ' + field)
        require(topology['observed_data_loss_seconds'] <= topology['rpo_seconds'], 'RPO exceeded')
        require(topology['observed_restore_seconds'] <= topology['rto_seconds'], 'RTO exceeded')
    return {'status': 'EVIDENCE_CONTRACT_SATISFIED', 'release_id': lock['release_id'],
            'scope': args.mode, 'note': 'Artifact/data checks are not independent attestation of supplied observations.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    freezing = commands.add_parser('freeze')
    freezing.add_argument('--root', type=Path, required=True)
    freezing.add_argument('--cluster', type=Path, required=True)
    freezing.add_argument('--operator-image', required=True)
    freezing.add_argument('--artifact', action='append', default=[])
    freezing.add_argument('--out', type=Path, required=True)
    checking = commands.add_parser('check')
    checking.add_argument('--root', type=Path, required=True)
    checking.add_argument('--lock', type=Path, required=True)
    checking.add_argument('--lock-sha256', required=True)
    checking.add_argument('--mode', choices=['preflight', 'acceptance', 'production'], default='preflight')
    checking.add_argument('--evidence', type=Path)
    checking.add_argument('--from-lock', type=Path)
    checking.add_argument('--from-lock-sha256')
    checking.add_argument('--from-root', type=Path)
    checking.add_argument('--edges', type=Path)
    checking.add_argument('--edges-sha256')
    checking.add_argument('--transition', choices=['upgrade', 'rollback'], default='upgrade')
    args = parser.parse_args()
    try:
        print(json.dumps(freeze(args) if args.command == 'freeze' else check(args), sort_keys=True))
        return 0
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as error:
        print(json.dumps({'status': 'BLOCKED', 'reason': str(error)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
