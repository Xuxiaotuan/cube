'use strict';

// Demo-only transport barriers. No SQL result, receipt, or router status is mocked.
const http = require('http');
const crypto = require('crypto');

function requestJson(url, { method = 'GET', body, headers = {}, timeout = 5000 } = {}) {
  return new Promise((resolve, reject) => {
    const bytes = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
    const req = http.request(url, { method, headers: {
      ...headers, ...(bytes ? { 'content-type': 'application/json', 'content-length': bytes.length } : {}),
    } }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => {
        try {
          const text = Buffer.concat(chunks).toString();
          const value = text ? JSON.parse(text) : null;
          if (res.statusCode < 200 || res.statusCode >= 300) {
            reject(new Error(`HTTP ${res.statusCode} ${url.pathname}: ${text.slice(0, 1000)}`));
          } else resolve(value);
        } catch (error) { reject(error); }
      });
    });
    const timer = setTimeout(() => req.destroy(new Error(`HTTP deadline: ${url.pathname}`)), timeout);
    req.on('close', () => clearTimeout(timer));
    req.on('error', reject);
    req.end(bytes);
  });
}

async function bodyBytes(req, max = 64 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > max) throw new Error('Demo request exceeds bounded buffer');
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function startProxy(options) {
  const { upstream, run, mode, port, controlPort, onBarrier, onRelease, onStale, onFailure } = options;
  const sockets = new Set();
  const websockets = new Set();
  const state = { mode, uploads: [], receipts: [], builds: [], fault: null, released: false };
  let claimed = false;
  let faultUpstream;
  let staleRejected = false;
  let unblock;
  const gate = new Promise(resolve => { unblock = resolve; });
  const fail = error => onFailure(error instanceof Error ? error : new Error(String(error)));
  function track(server) {
    server.on('connection', socket => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });
  }

  async function hold(detail, req, res) {
    state.fault = detail;
    res.on('close', () => {
      if (!state.released) fail(new Error('Upload client disconnected before fault barrier was released'));
    });
    await onBarrier(detail);
    await gate;
    req.destroy();
    res.destroy(); // Deliberately lose the real upstream acknowledgement.
  }

  const server = http.createServer((incoming, outgoing) => {
    (async () => {
      const url = new URL(incoming.url, upstream);
      const upload = incoming.method === 'POST' && url.pathname === '/upload-temp-file';
      const fault = upload && !claimed;
      if (fault) claimed = true;
      const headers = { ...incoming.headers, host: upstream.host };
      // One fresh HTTP connection per request follows the updated leader Service.
      delete headers.connection;
      let payload;
      let detail;
      if (upload) {
        payload = await bodyBytes(incoming);
        const sha256 = crypto.createHash('sha256').update(payload).digest('hex');
        const name = url.searchParams.get('name');
        if (name !== `${sha256}.csv.gz` || url.searchParams.get('sha256') !== sha256) {
          throw new Error('Upload must use immutable name and SHA256 over compressed bytes');
        }
        if (payload.length < 2) throw new Error('Upload is too small for an in-flight barrier');
        detail = { name, sha256, size: payload.length, bytesForwarded: 0, blockedLastByte: false };
        state.uploads.push({ name, sha256, size: payload.length, afterFault: state.released });
        delete headers['transfer-encoding'];
        headers['content-length'] = payload.length;
      }

      const remote = http.request(url, { method: incoming.method, headers, agent: false }, response => {
        (async () => {
          if (fault && !mode.startsWith('receipt-')) {
            response.resume();
            if (!state.released) throw new Error('Upstream completed an upload with a withheld body byte');
            return;
          }
          const collect = fault || /\/(upload-temp-file-status|router\/build-status)$/.test(url.pathname);
          if (!collect) {
            outgoing.writeHead(response.statusCode, response.headers);
            response.pipe(outgoing);
            return;
          }
          const bytes = await bodyBytes(response);
          if (url.pathname === '/upload-temp-file-status' && response.statusCode === 200) {
            state.receipts.push({ name: url.searchParams.get('name'), ...JSON.parse(bytes), afterFault: state.released });
          }
          if (url.pathname === '/router/build-status' && response.statusCode === 200) {
            state.builds.push({ table: url.searchParams.get('table'), ...JSON.parse(bytes), afterFault: state.released });
          }
          if (fault) {
            if (response.statusCode < 200 || response.statusCode >= 300) {
              throw new Error(`Remote upload failed before receipt barrier: ${response.statusCode} ${bytes}`);
            }
            const receipt = await requestJson(new URL(`/upload-temp-file-status?name=${detail.name}&sha256=${detail.sha256}`, upstream));
            if (receipt.state !== 'uploaded' || receipt.sha256 !== detail.sha256 || receipt.size !== detail.size) {
              throw new Error('Remote upload receipt did not confirm compressed bytes and size');
            }
            await hold({ ...detail, bytesForwarded: detail.size, upstreamStatus: response.statusCode, receipt }, remote, outgoing);
          } else {
            outgoing.writeHead(response.statusCode, response.headers);
            outgoing.end(bytes);
          }
        })().catch(error => { fail(error); outgoing.destroy(); });
      });
      remote.on('error', error => {
        // Once the barrier is established, leader deletion is the intended error.
        if (fault && state.fault) return;
        outgoing.destroy(error);
      });
      if (fault) faultUpstream = remote;
      incoming.on('aborted', () => remote.destroy());
      if (fault && !mode.startsWith('receipt-')) {
        remote.flushHeaders();
        await new Promise((resolve, reject) => remote.write(payload.subarray(0, -1), error => error ? reject(error) : resolve()));
        await hold({ ...detail, bytesForwarded: payload.length - 1, blockedLastByte: true }, remote, outgoing);
      } else if (payload) remote.end(payload);
      else incoming.pipe(remote);
    })().catch(error => { fail(error); outgoing.destroy(); });
  });

  // Transparent websocket tunnel, including upgrade headers and binary frames.
  // It neither decodes nor replays writes. Release destroys every old tunnel.
  server.on('upgrade', (req, socket, head) => {
    const remote = http.request(new URL(req.url, upstream), {
      headers: { ...req.headers, host: upstream.host }, agent: false,
    });
    remote.on('upgrade', (res, peer, remoteHead) => {
      websockets.add(socket);
      websockets.add(peer);
      socket.write(`HTTP/1.1 ${res.statusCode} ${res.statusMessage}\r\n${res.rawHeaders.reduce((all, value, i, values) => i % 2 ? all : `${all}${value}: ${values[i + 1]}\r\n`, '')}\r\n`);
      if (head.length) peer.write(head);
      if (remoteHead.length) socket.write(remoteHead);
      socket.pipe(peer).pipe(socket);
      const close = () => { websockets.delete(socket); websockets.delete(peer); socket.destroy(); peer.destroy(); };
      socket.on('error', close); peer.on('error', close);
      socket.on('close', close); peer.on('close', close);
    });
    remote.on('response', res => { res.resume(); socket.destroy(); });
    remote.on('error', () => socket.destroy());
    socket.on('error', () => remote.destroy());
    remote.end();
  });

  const control = http.createServer((req, res) => {
    (async () => {
      if (req.headers['x-ha-run'] !== run) { res.writeHead(403); res.end(); return; }
      let value;
      if (req.method === 'GET' && req.url === '/state') value = state;
      else if (req.method === 'POST' && req.url === '/stale') {
        value = await onStale();
        staleRejected = value?.rejected === true;
      } else if (req.method === 'POST' && req.url === '/drain-upload-abort') {
        if (mode !== 'drain-failover' || !state.fault || state.released || state.drainUploadAborted || !staleRejected || !faultUpstream) {
          throw new Error('Drain upload abort requires an unreleased fault and proven stale-writer rejection');
        }
        // Cancel only the admitted request on the OLD router so drain can
        // finish. Keep the API-facing response held until cutover /release.
        faultUpstream.destroy();
        state.drainUploadAborted = true;
        value = { upstreamAborted: true, downstreamHeld: true, released: false };
      }
      else if (req.method === 'POST' && req.url === '/release') {
        if (!state.fault || state.released) throw new Error('No unreleased fault barrier');
        const proof = JSON.parse((await bodyBytes(req, 16384)).toString());
        await onRelease(proof);
        state.released = true;
        for (const socket of websockets) socket.destroy();
        unblock();
        value = { released: true };
      } else if (req.method === 'POST' && req.url === '/abort') {
        fail(new Error('Local fault controller aborted the run'));
        value = { aborted: true };
      } else { res.writeHead(404); res.end(); return; }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(value));
    })().catch(error => {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
      fail(error);
    });
  });
  track(server); track(control);
  for (const [listener, listenPort] of [[server, port], [control, controlPort]]) {
    await new Promise((resolve, reject) => {
      listener.once('error', reject);
      listener.listen(listenPort, '127.0.0.1', resolve);
    });
  }
  return {
    state,
    port: server.address().port,
    controlPort: control.address().port,
    close() {
      if (faultUpstream) faultUpstream.destroy();
      state.released = true;
      unblock();
      for (const socket of [...sockets, ...websockets]) socket.destroy();
      server.close(); control.close();
    },
  };
}

module.exports = { startProxy, requestJson };
