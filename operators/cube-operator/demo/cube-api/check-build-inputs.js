'use strict';

// Input guard only: never compile, load native modules, or change the workspace.
const fs = require('fs');
const path = require('path');
const root = process.argv[2];
const packages = ['cubejs-api-gateway', 'cubejs-base-driver', 'cubejs-backend-shared',
  'cubejs-cubestore-driver', 'cubejs-query-orchestrator', 'cubejs-schema-compiler',
  'cubejs-server', 'cubejs-server-core', 'cubejs-backend-cloud', 'cubejs-templates', 'cubejs-backend-native'];
const failures = [];
for (const name of packages) {
  const dir = path.join(root, 'packages', name);
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
  if (!fs.existsSync(path.join(dir, manifest.main))) failures.push(`${name}: missing ${manifest.main}`);
}
// Check only the recovery-critical compiled modules, not unrelated test/source
// timestamps across the monorepo. These tsconfigs emit dist/src, not lib/.
for (const relative of [
  'cubejs-cubestore-driver/src/CubeStoreDriver.ts',
  'cubejs-cubestore-driver/src/WebSocketConnection.ts',
  'cubejs-cubestore-driver/src/PreAggregationBuildStore.ts',
  'cubejs-query-orchestrator/src/orchestrator/PreAggregationLoader.ts',
]) {
  const source = path.join(root, 'packages', relative);
  const output = path.join(root, 'packages', relative.replace('/src/', '/dist/src/').replace(/\.ts$/, '.js'));
  if (!fs.existsSync(output)) failures.push(`missing ${output}`);
  else if (fs.statSync(source).mtimeMs > fs.statSync(output).mtimeMs + 1) failures.push(`source newer than ${output}`);
}
const nativeHeader = Buffer.alloc(20);
const fd = fs.openSync(process.argv[3], 'r');
try { fs.readSync(fd, nativeHeader, 0, nativeHeader.length, 0); } finally { fs.closeSync(fd); }
if (nativeHeader.subarray(0, 4).toString('hex') !== '7f454c46' || nativeHeader[4] !== 2 || nativeHeader[5] !== 1 || ![62, 183].includes(nativeHeader.readUInt16LE(18))) {
  failures.push('NATIVE_INDEX_NODE must be a Linux ELF64 x86_64/aarch64 module, not a host macOS module');
}
if (failures.length) {
  console.error(`Refusing stale API image inputs. Rebuild workspace Node packages first:\n${failures.join('\n')}`);
  process.exitCode = 1;
}
