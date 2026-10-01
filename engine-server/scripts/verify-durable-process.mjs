import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFile, mkdtemp, copyFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { delimiter, join, resolve } from 'node:path';

// Only creates and removes its own disposable MySQL container. Credentials stay in process env.
const docker = process.env.FLOW_PROBE_DOCKER || 'docker';
const label = `flow-recovery-probe-${randomBytes(6).toString('hex')}`;
const password = randomBytes(24).toString('hex');
const xml = await readFile('engine-server-starter/target/surefire-reports/TEST-com.playrewards.flowengine.server.recovery.DurableRecoveryTest.xml', 'utf8');
const reportClasspath = xml.match(/<property name="java.class.path" value="([^"]+)"/)[1]
  .replaceAll('&amp;', '&').replaceAll('&quot;', '"');
const currentModules = [
  'engine-contract', 'engine-spi', 'engine-core', 'engine-adapters', 'engine-server-starter', 'engine-server',
].map(module => resolve(module, 'target/classes'));
const classpath = [...currentModules, reportClasspath].join(delimiter);
const classes = await mkdtemp(join(tmpdir(), 'flow-recovery-probe-'));
let container;
const children = new Set();

function run(command, args, env = process.env, timeout = 60000) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
    children.add(child);
    let output = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), timeout);
    const collect = chunk => {
      output += chunk.toString();
      if (output.length > 1048576) child.kill('SIGKILL');
    };
    child.stdout.on('data', collect); child.stderr.on('data', collect);
    child.on('error', error => { clearTimeout(timer); children.delete(child); reject(error); });
    child.on('close', (code, signal) => {
      clearTimeout(timer); children.delete(child);
      if (code === 0) resolve(output.trim());
      else reject(new Error(`${command} failed (${code ?? signal}): ${output.replaceAll(password, '[redacted]').slice(-5000)}`));
    });
  });
}

async function verifyServer(jar, jdbcUrl) {
  const fixedJar = join(classes, 'server.jar');
  await copyFile(jar, fixedJar); // Never execute a target jar which a parallel package can replace.
  const listener = createServer();
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const configuration = {
    'server.address': '127.0.0.1', 'server.port': port, 'flow-engine.demo.enabled': true,
    'flow-engine.durable.enabled': true, 'flow-engine.durable.jdbc-url': jdbcUrl,
    'flow-engine.durable.username': 'root', 'flow-engine.durable.password': password,
  };
  async function start() {
    const process = spawn('java', ['-jar', fixedJar], {
      env: { ...globalThis.process.env, SPRING_APPLICATION_JSON: JSON.stringify(configuration) },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    children.add(process);
    let output = '';
    for (const stream of [process.stdout, process.stderr]) stream.on('data', chunk => {
      output = (output + chunk).slice(-100000);
    });
    const closed = new Promise(resolve => process.once('close', (code, signal) => {
      children.delete(process); resolve({ code, signal });
    }));
    process.once('error', () => {});
    const until = Date.now() + 30000;
    for (;;) {
      try {
        const response = await fetch(`${base}/actuator/health`, { signal: AbortSignal.timeout(1000) });
        if (response.ok && (await response.json()).status === 'UP') break;
      } catch {}
      if (Date.now() > until || process.exitCode !== null || process.signalCode !== null) {
        throw new Error(`durable server not ready: ${output.replaceAll(password, '[redacted]')}`);
      }
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    return async () => { process.kill('SIGKILL'); assert.equal((await closed).signal, 'SIGKILL'); };
  }
  async function request(path, body, headers = {}) {
    const response = await fetch(`${base}${path}`, { method: body ? 'POST' : 'GET',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(35000) });
    assert.ok(response.ok, `${path}: ${response.status} ${response.ok ? '' : await response.text()}`);
    return response;
  }
  const json = async (path, body) => (await request(path, body)).json();
  const events = text => text.replaceAll('\r\n', '\n').split('\n\n').flatMap(frame => {
    const value = frame.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5)).join('\n');
    return value ? [JSON.parse(value)] : [];
  });
  let stop = await start();
  try {
    const wait = await json('/v1/executions', { flowReference: 'demo:all', mode: 'DURABLE', responseMode: 'WAIT' });
    assert.equal(wait.status, 'SUCCEEDED');
    const payload = await json(`/v1/payloads/${wait.resultPayloadRef}`);
    assert.deepEqual(payload, { results: [10, 20], child: { message: 'child-complete' }, selected: 'stream-complete', http: { message: 'demo-http' } });
    const nodeRecords = await json(`/v1/executions/${wait.executionId}/nodes?limit=500`);
    assert.equal(new Set(nodeRecords.map(node => node.componentType)).size, 11);
    const streamed = events(await (await request('/v1/executions/stream', { flowReference: 'demo:all', mode: 'DURABLE' })).text());
    assert.equal(streamed.at(-1).eventType, 'COMPLETED');
    assert.equal(streamed.filter(event => event.eventType === 'CONTENT').length, 2);
    const cursor = streamed[Math.floor(streamed.length / 2)];
    await stop(); stop = await start();
    assert.equal((await json(`/v1/executions/${wait.executionId}`)).status, 'SUCCEEDED');
    assert.deepEqual(await json(`/v1/payloads/${wait.resultPayloadRef}`), payload);
    const replay = events(await (await request(`/v1/executions/${cursor.executionId}/stream`, undefined, { 'Last-Event-ID': cursor.eventId })).text());
    assert.deepEqual(replay.map(event => event.eventId), streamed.filter(event => event.sequence > cursor.sequence).map(event => event.eventId));
    const bodyNode = nodeRecords.find(node => node.nodeCode === 'body');
    const logs = await json(`/v1/node-executions/${bodyNode.nodeExecutionId}/logs`);
    const diagnostic = logs.find(log => log.maskedAttributes.diagnosticPayloadRef);
    assert.ok(diagnostic, 'node logs must survive process restart');
    assert.ok((await json(`/v1/payloads/${diagnostic.maskedAttributes.diagnosticPayloadRef}`)).message.includes('循环演示'));
    return { wait: wait.executionId, stream: cursor.executionId, nodeTypes: 11, events: streamed.length,
      restartQueries: 'passed', restartSseReplay: 'passed', restartLogs: 'passed' };
  } finally { await stop(); }
}

try {
  await run('javac', ['-cp', classpath, '-d', classes,
    'engine-server/src/test/java/com/playrewards/flowengine/server/recovery/ProcessRecoveryProbe.java']);
  container = await run(docker, ['run', '-d', '--name', label, '--label', `flow-probe=${label}`,
    '-p', '127.0.0.1::3306', '-e', 'MYSQL_ROOT_PASSWORD', '-e', 'MYSQL_DATABASE=probe', 'mysql:8.4'],
  { ...process.env, MYSQL_ROOT_PASSWORD: password });
  assert.match(container, /^[a-f0-9]{64}$/);
  const portText = await run(docker, ['port', container, '3306/tcp']);
  const port = portText.match(/^127\.0\.0\.1:(\d+)$/m)?.[1];
  assert.ok(port, 'database must bind loopback only');
  const readyUntil = Date.now() + 60000;
  for (;;) {
    try {
      await run(docker, ['exec', '-e', 'MYSQL_PWD', container, 'mysql', '-uroot', '-Dprobe', '-e', 'SELECT 1'],
        { ...process.env, MYSQL_PWD: password }, 5000);
      break;
    } catch (error) {
      if (Date.now() > readyUntil) throw error;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  const env = { ...process.env, FLOW_PROBE_JDBC_URL: `jdbc:mysql://127.0.0.1:${port}/probe?allowPublicKeyRetrieval=true&useSSL=false`,
    FLOW_PROBE_DB_USER: 'root', FLOW_PROBE_DB_PASSWORD: password };
  const javaArgs = ['-cp', `${classes}:${classpath}`, 'com.playrewards.flowengine.server.recovery.ProcessRecoveryProbe'];
  const receipts = [];
  // START, first business result and second business result are three distinct committed checkpoints.
  for (const checkpoint of [1, 2, 3]) {
    const tenant = `process-probe-${randomBytes(8).toString('hex')}`;
    const child = spawn('java', [...javaArgs, 'start', tenant, String(checkpoint)], { env, stdio: ['ignore', 'pipe', 'pipe'] });
    children.add(child);
    let output = '';
    const closed = new Promise(resolve => child.once('close', (code, signal) => { children.delete(child); resolve({ code, signal }); }));
    const executionId = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`checkpoint ${checkpoint} timed out`)), 30000);
      const collect = chunk => {
        output += chunk.toString();
        const id = output.match(/^CHECKPOINT ([a-f0-9-]+)$/m)?.[1];
        if (id) { clearTimeout(timer); resolve(id); }
        if (output.length > 1048576) { clearTimeout(timer); reject(new Error('probe output exceeded bound')); }
      };
      child.stdout.on('data', collect); child.stderr.on('data', collect);
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.once('close', () => { clearTimeout(timer); reject(new Error(output.replaceAll(password, '[redacted]').slice(-5000))); });
    });
    child.kill('SIGKILL');
    assert.equal((await closed).signal, 'SIGKILL');
    // Real abrupt death: allow the database-clock lease to expire, without graceful release.
    await new Promise(resolve => setTimeout(resolve, 2300));
    const resumed = await run('java', [...javaArgs, 'resume', tenant, executionId], env);
    const receipt = JSON.parse(resumed.match(/^RECOVERED (.+)$/m)?.[1] || 'null');
    assert.equal(receipt?.executionId, executionId);
    assert.equal(receipt?.status, 'SUCCEEDED');
    receipts.push({ checkpoint, ...receipt });
  }
  const jarIndex = process.argv.indexOf('--server-jar');
  const server = jarIndex < 0 ? undefined : await verifyServer(process.argv[jarIndex + 1], env.FLOW_PROBE_JDBC_URL);
  console.log(JSON.stringify({ kind: 'independent-jvm-sigkill', checkpoints: receipts, server }, null, 2));
} finally {
  for (const child of children) child.kill('SIGKILL');
  if (container) {
    const owner = await run(docker, ['inspect', '--format', '{{index .Config.Labels "flow-probe"}}', container]);
    assert.equal(owner, label, 'only remove this run-owned disposable container');
    await run(docker, ['rm', '-f', '-v', container]);
  }
}
