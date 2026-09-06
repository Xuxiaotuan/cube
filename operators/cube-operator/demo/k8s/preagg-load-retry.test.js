'use strict';

// PURE harness tests only. These are not Cube API or HA runtime evidence.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const http = require('node:http');
const source = fs.readFileSync(require('node:path').join(__dirname, 'preagg-fault-check.js'), 'utf8');
const functions = source.slice(source.indexOf('function transientLoadError('), source.indexOf('async function main()'));
const sandbox = { require, Buffer, Date, setTimeout, clearTimeout };
vm.runInNewContext(functions, sandbox);
const error = (message, statusCode = 400) => Object.assign(new Error(message), { statusCode });
const draining = () => error('ConnectionError: CubeStore connection error: WrongConnection: Router is draining');

test('PURE load policy: explicit transients only, arbitrary 400/auth/SQL errors fail closed', () => {
  for (const item of [draining(), error('MutationUnknown: result unknown'), error('NotLeader: cutover'),
    error('bad gateway', 502), error('unavailable', 503), Object.assign(new Error('reset'), { code: 'ECONNRESET' })]) {
    assert.equal(sandbox.transientLoadError(item), true, item.message);
  }
  for (const item of [error('SQL syntax error'), error('Invalid query'), error('permission denied'),
    error('SQL syntax error near MutationUnknown'), error('ConnectionError: CubeStore connection error: denied', 403),
    error('NotLeader: unavailable', 401), error('Unknown server error', 500)]) {
    assert.equal(sandbox.transientLoadError(item), false, item.message);
  }
});

function fixture(sequence, deadline = 600000) {
  let now = 1000;
  let calls = 0;
  const events = [];
  const availability = { count: 0, firstAt: null, lastAt: null, recoveredAt: null, windowMs: 0 };
  return { events, availability, calls: () => calls, execute: () => sandbox.pollLoadResponse({
    deadline, availability, now: () => now, sleep: async ms => { now += ms; },
    emit: (name, detail) => events.push({ name, detail }),
    request: async timeout => {
      assert.ok(timeout > 0 && timeout <= 45000);
      const value = sequence[Math.min(calls++, sequence.length - 1)];
      if (value instanceof Error) throw value;
      return value;
    },
  }) };
}

test('PURE load polling: draining/503/ContinueWait recover with counted evidence', async () => {
  const expected = { data: [{ count: '1' }] };
  const f = fixture([draining(), error('unavailable', 503), error('Continue wait', 200), expected]);
  const result = await f.execute();
  assert.equal(result.result, expected);
  assert.equal(result.attempts, 4);
  assert.equal(f.availability.count, 2);
  assert.equal(f.availability.windowMs, 750);
  assert.equal(f.events.filter(event => event.name === 'api_transient_unavailable').length, 2);
  assert.equal(f.events.at(-1).name, 'api_availability_recovered');
});

test('PURE load polling: ordinary errors stop immediately without retry', async () => {
  const f = fixture([error('Invalid query'), { data: [] }]);
  await assert.rejects(f.execute(), /Invalid query/);
  assert.equal(f.calls(), 1);
  assert.equal(f.availability.count, 0);
});

test('PURE load polling: observed receipt-loss MutationUnknownError retries only the load request', async () => {
  const observed = error('MutationUnknownError: CubeStore connection closed after sending a non-idempotent request; mutation outcome is unknown');
  const response = { data: [{ count: '1' }] };
  const f = fixture([observed, response]);
  const result = await f.execute();
  assert.equal(result.result, response);
  assert.equal(result.attempts, 2);
  assert.equal(f.availability.count, 1);
  assert.equal(f.events[0].name, 'api_transient_unavailable');
  assert.equal(f.events[0].detail.error, observed.message);
  assert.equal(sandbox.transientLoadError(error('SQL error near MutationUnknownError')), false);
  assert.equal(sandbox.transientLoadError(error('MutationUnknownError: permission denied', 403)), false);
});

test('PURE load polling: continuous transient failure is bounded by 120s and original deadline', async () => {
  for (const [deadline, count] of [[600000, 480], [2000, 4]]) {
    const f = fixture([draining()], deadline);
    await assert.rejects(f.execute(), /load deadline exceeded/);
    assert.equal(f.calls(), count);
    assert.equal(f.availability.count, count);
  }
});

test('PURE load HTTP transport: retains HTML 502/503, refuses HTML 400 and preserves same request', async t => {
  let status = 502;
  const bodies = [];
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      bodies.push(body);
      res.writeHead(status, { 'content-type': 'text/html' });
      res.end('<html>unavailable</html>');
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const query = { measures: ['SameRun.count'] };
  for (const code of [502, 503, 400]) {
    status = code;
    await assert.rejects(sandbox.requestLoad(new URL(`http://127.0.0.1:${server.address().port}/load`), {
      body: { query }, headers: {}, timeout: 1000,
    }), failure => failure.statusCode === code && sandbox.transientLoadError(failure) === (code !== 400));
  }
  assert.deepEqual(bodies, Array(3).fill(JSON.stringify({ query })));
});
