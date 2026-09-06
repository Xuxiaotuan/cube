'use strict';

// PURE proof-validation fixtures. No simulated frame is runtime fencing proof.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, 'preagg-fault-check.js'), 'utf8');
const sandbox = { assert };
vm.runInNewContext(source.slice(source.indexOf('function assertStaleWriterFrameProof('), source.indexOf('async function observeStaleWriter(')), sandbox);
const sql = 'INSERT INTO ha_test_source.events VALUES (1)';
const valid = () => ({ sql, messageId: 2,
  sent: { command: 'HttpQuery', messageId: 2, sql },
  response: { command: 'HttpError', messageId: 2, error: 'WrongConnection: Router is draining' },
  error: { name: 'ConnectionError', message: 'CubeStore connection error: WrongConnection: Router is draining' },
});

test('PURE stale writer: accepts only matching request/server frame and exact TS drain mapping', () => {
  assert.doesNotThrow(() => sandbox.assertStaleWriterFrameProof(valid()));
});

test('PURE stale writer: transport/unknown/timeout/errors alone cannot prove fencing', () => {
  for (const error of [
    { name: 'ConnectionError', message: 'CubeStore connection error: ECONNRESET', code: 'ECONNRESET' },
    { name: 'ConnectionError', message: 'CubeStore connection error: ETIMEDOUT', code: 'ETIMEDOUT' },
    { name: 'ConnectionError', message: 'CubeStore connection closed before request could be sent', code: 'MUTATION_NOT_DISPATCHED' },
    { name: 'MutationUnknownError', message: 'CubeStore connection closed after sending a non-idempotent request; mutation outcome is unknown', code: 'MUTATION_UNKNOWN' },
    { name: 'ConnectionError', message: 'CubeStore connection error: WrongConnection: Router is draining' },
  ]) assert.throws(() => sandbox.assertStaleWriterFrameProof({ ...valid(), response: null, error }));
  assert.throws(() => sandbox.assertStaleWriterFrameProof({ ...valid(), error: { ...valid().error, code: 'ECONNRESET' } }));
});

test('PURE stale writer: rejects mismatched IDs/SQL/response types and missing or unrelated refusals', () => {
  for (const mutate of [
    p => { p.sent = null; },
    p => { p.sent.messageId++; },
    p => { p.sent.sql = 'SELECT 1'; },
    p => { p.response.messageId++; },
    p => { p.response.command = 'HttpResultSet'; },
    p => { p.response.error = 'WrongConnection: lease expired'; },
    p => { p.error = null; },
    p => { p.error.message = 'Connection timed out'; },
    p => { p.captureError = 'decode failed'; },
  ]) {
    const p = valid(); mutate(p);
    assert.throws(() => sandbox.assertStaleWriterFrameProof(p));
  }
});
