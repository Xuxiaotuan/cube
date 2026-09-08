'use strict';

// Pure assertion tests only; they are not Kubernetes or publication evidence.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, 'preagg-fault-check.js'), 'utf8');
const sandbox = { assert };
vm.runInNewContext(source.slice(source.indexOf('function assertAtomicLedgerProof('), source.indexOf('async function observeAtomicLedger(')), sandbox);

function fixture() {
  const pointer = { key: 'rollups.events_c_s_123', generation: '9007199254740993', owner: 'builder' };
  const record = {
    buildId: pointer.key, phase: 'ready', atomicLedger: pointer, tableId: '9007199254740995',
    versionEntry: { content_version: 'c', structure_version: 's' }, create: { params: ['temp-uploads/a'] },
  };
  const ledger = { ...pointer, state: 'published', manifest: {
    schema: 'rollups', table: 'events_c_s_123', tableId: record.tableId,
    contentVersion: 'c', structureVersion: 's', locations: ['temp-uploads/a'],
  } };
  const status = { state: 'ready', tableId: record.tableId, locations: ['temp-uploads/a'] };
  return { record, ledger, status };
}

test('PURE atomic proof requires authoritative publication and independent physical identity', () => {
  const { record, ledger, status } = fixture();
  assert.doesNotThrow(() => sandbox.assertAtomicLedgerProof(record, ledger, status));
});

test('PURE atomic proof rejects cache-only ready, stale identity and mismatched physical data', () => {
  for (const mutate of [
    p => { p.ledger = null; },
    p => { p.record.atomicLedger = null; },
    p => { p.ledger.state = 'bound'; },
    p => { p.ledger.generation = '2'; },
    p => { p.ledger.generation = 2; },
    p => { p.ledger.owner = 'stale'; },
    p => { p.status = null; },
    p => { p.status.tableId = '2'; },
    p => { p.status.locations = ['different-file']; },
    p => { p.ledger.manifest.contentVersion = 'other'; },
  ]) {
    const proof = fixture(); mutate(proof);
    assert.throws(() => sandbox.assertAtomicLedgerProof(proof.record, proof.ledger, proof.status));
  }
});
