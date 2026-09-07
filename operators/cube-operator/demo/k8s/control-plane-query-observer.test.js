'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { inspectData } = require('./control-plane-query-observer');

function rows() {
  return Array.from({ length: 16 }, (_, i) => ({
    'RouterHaRollup.bucket': `bucket-${i}`, 'RouterHaRollup.rowCount': '2',
    'RouterHaRollup.totalAmount': '10', 'RouterHaRollup.idChecksum': '20',
  }));
}

test('checks exact aggregate totals and canonical order', () => {
  assert.deepEqual(inspectData(rows(), ['32', '160', '320']), inspectData(rows().reverse(), ['32', '160', '320']));
});
test('rejects missing and duplicated groups', () => {
  assert.throws(() => inspectData(rows().slice(1), ['32', '160', '320']));
  const data = rows(); data[1] = data[0];
  assert.throws(() => inspectData(data, ['32', '160', '320']));
});
test('rejects incorrect business totals', () => {
  assert.throws(() => inspectData(rows(), ['33', '160', '320']), /mismatch/);
});
test('does not silently round large integer aggregates', () => {
  const data = rows(); data[0]['RouterHaRollup.totalAmount'] = '9007199254740993';
  assert.equal(inspectData(data, ['32', '9007199254741143', '320']).totals[1], '9007199254741143');
});
test('rejects non-integer values', () => {
  const data = rows(); data[0]['RouterHaRollup.idChecksum'] = 'NaN';
  assert.throws(() => inspectData(data, ['32', '160', '320']), /Non-integer/);
});

test('HTTP success without data fails the CLI even after valid queries resume', { timeout: 20000 }, async t => {
  const http = require('node:http');
  const fs = require('node:fs/promises');
  const os = require('node:os');
  const path = require('node:path');
  const { spawn } = require('node:child_process');
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'cube-observer-http-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const preload = path.join(directory, 'redirect.cjs');
  await fs.writeFile(preload, `const http = require('node:http');
const request = http.request;
http.request = (options, callback) => request.call(http, { ...options, port: Number(process.env.CUBE_HA_TEST_PORT) }, callback);
`);
  let requests = 0;
  const server = http.createServer((request, response) => {
    request.resume();
    requests++;
    const payload = requests === 2 ? {} : requests === 3 ? { data: null } : requests === 4 ? { data: [] } : { data: rows() };
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(payload));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const child = spawn(process.execPath, ['--require', preload, path.join(__dirname, 'control-plane-query-observer.js')], {
    env: { ...process.env, CUBE_HA_TEST_PORT: String(server.address().port), HA_RUN: 'r0123456789abcdef',
      OBSERVE_SECONDS: '10', EXPECTED_ROWS: '32', EXPECTED_AMOUNT: '160', EXPECTED_CHECKSUM: '320',
      CUBEJS_API_SECRET: 'test-only-observer-key' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  let output = ''; let errors = '';
  child.stdout.on('data', chunk => { output += chunk; });
  child.stderr.on('data', chunk => { errors += chunk; });
  const exit = await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
  assert.equal(errors, '');
  const events = output.trim().split('\n').map(line => JSON.parse(line));
  const result = events.at(-1);
  assert.equal(result.event, 'result');
  assert.equal(result.mismatch, 3);
  assert.equal(events.filter(event => event.event === 'data_mismatch').length, 3);
  assert.equal(events.at(-2).event, 'query', 'later valid reads must not erase earlier malformed data');
  assert.equal(result.status, 'FAIL');
  assert.equal(exit.code, 1);
  assert.equal(exit.signal, null);
});
