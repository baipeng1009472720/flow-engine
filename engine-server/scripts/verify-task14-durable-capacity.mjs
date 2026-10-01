import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { delimiter, join, resolve } from 'node:path';

// This gate owns and removes one disposable database. Credentials never leave process arguments/env.
const concurrency = Number.parseInt(process.env.FLOW_TASK14_CONCURRENCY || '100', 10);
const maxHeapGrowthBytes = 512 * 1024 * 1024;
const maxPlatformThreadGrowth = 96;
const maxNodePermitInFlight = 256;
const maxStorageCapacityInFlight = 256;
assert.ok(
  Number.isInteger(concurrency) && concurrency >= 1 && concurrency <= 100,
  'diagnostic concurrency must be between 1 and the Task 14 gate of 100',
);
const docker = process.env.FLOW_PROBE_DOCKER || 'docker';
const label = `flow-task14-${randomBytes(6).toString('hex')}`;
const tenant = `process-probe-task14-${randomBytes(8).toString('hex')}`;
const password = randomBytes(24).toString('hex');
const xml = await readFile(
  'engine-server-starter/target/surefire-reports/TEST-com.playrewards.flowengine.server.recovery.DurableRecoveryTest.xml',
  'utf8',
);
const reportClasspath = xml.match(/<property name="java.class.path" value="([^"]+)"/)[1]
  .replaceAll('&amp;', '&').replaceAll('&quot;', '"');
const currentModules = [
  'engine-contract', 'engine-spi', 'engine-core', 'engine-adapters', 'engine-server-starter', 'engine-server',
].map(module => resolve(module, 'target/classes'));
const classpath = [...currentModules, reportClasspath].join(delimiter);
const classes = await mkdtemp(join(tmpdir(), 'flow-task14-probe-'));
const children = new Set();
let container;

function run(command, args, env = process.env, timeout = 120000) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
    children.add(child);
    let output = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), timeout);
    const collect = chunk => {
      output += chunk.toString();
      if (output.length > 2 * 1024 * 1024) child.kill('SIGKILL');
    };
    child.stdout.on('data', collect);
    child.stderr.on('data', collect);
    child.on('error', error => {
      clearTimeout(timer);
      children.delete(child);
      reject(error);
    });
    child.on('close', (code, signal) => {
      clearTimeout(timer);
      children.delete(child);
      if (code === 0) resolve(output.trim());
      else reject(new Error(`${command} failed (${code ?? signal}): ${redact(output).slice(-8000)}`));
    });
  });
}

function spawnUntil(prefix, args, env, timeout = 60000) {
  const child = spawn('java', args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
  children.add(child);
  let output = '';
  const closed = new Promise(resolve => child.once('close', (code, signal) => {
    children.delete(child);
    resolve({ code, signal });
  }));
  const marker = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`missing ${prefix}: ${redact(output).slice(-8000)}`)), timeout);
    const collect = chunk => {
      output += chunk.toString();
      const line = output.split(/\r?\n/).find(value => value.startsWith(prefix));
      if (line) {
        clearTimeout(timer);
        resolve(JSON.parse(line.slice(prefix.length)));
      }
      if (output.length > 2 * 1024 * 1024) {
        clearTimeout(timer);
        reject(new Error('probe output exceeded bound'));
      }
    };
    child.stdout.on('data', collect);
    child.stderr.on('data', collect);
    child.once('error', reject);
    child.once('close', () => {
      clearTimeout(timer);
      reject(new Error(`probe exited before ${prefix}: ${redact(output).slice(-8000)}`));
    });
  });
  return { child, closed, marker };
}

function redact(value) {
  return value.replaceAll(password, '[redacted]');
}

try {
  await run('javac', [
    '-cp', classpath,
    '-d', classes,
    'engine-server/src/test/java/com/playrewards/flowengine/server/recovery/ProcessRecoveryProbe.java',
  ]);
  container = await run(docker, [
    'run', '-d', '--name', label, '--label', `flow-probe=${label}`,
    '-p', '127.0.0.1::3306', '-e', 'MYSQL_ROOT_PASSWORD', '-e', 'MYSQL_DATABASE=probe', 'mysql:8.4',
  ], { ...process.env, MYSQL_ROOT_PASSWORD: password });
  assert.match(container, /^[a-f0-9]{64}$/);
  const portText = await run(docker, ['port', container, '3306/tcp']);
  const port = portText.match(/^127\.0\.0\.1:(\d+)$/m)?.[1];
  assert.ok(port, 'database must bind loopback only');

  const readyUntil = Date.now() + 60000;
  for (;;) {
    try {
      await run(
        docker,
        ['exec', '-e', 'MYSQL_PWD', container, 'mysql', '-uroot', '-Dprobe', '-e', 'SELECT 1'],
        { ...process.env, MYSQL_PWD: password },
        5000,
      );
      break;
    } catch (error) {
      if (Date.now() > readyUntil) throw error;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  const env = {
    ...process.env,
    FLOW_PROBE_JDBC_URL: `jdbc:mysql://127.0.0.1:${port}/probe?allowPublicKeyRetrieval=true&useSSL=false`,
    FLOW_PROBE_DB_USER: 'root',
    FLOW_PROBE_DB_PASSWORD: password,
  };
  const java = ['-cp', `${classes}:${classpath}`, 'com.playrewards.flowengine.server.recovery.ProcessRecoveryProbe'];
  const first = spawnUntil(
    'BATCH_CHECKPOINT ',
    [...java, 'batch-start', tenant, String(concurrency)],
    env,
    60000,
  );
  let checkpoint;
  try {
    checkpoint = await first.marker;
  } catch (failure) {
    const innodb = await run(
      docker,
      ['exec', '-e', 'MYSQL_PWD', container, 'mysql', '-uroot', '-Dprobe', '-e', 'SHOW ENGINE INNODB STATUS\\G'],
      { ...process.env, MYSQL_PWD: password },
      10000,
    );
    throw new Error(`${failure.message}\nINNODB_DIAGNOSTIC\n${innodb.slice(-16000)}`);
  }
  assert.equal(checkpoint.executionIds.length, concurrency);
  first.child.kill('SIGKILL');
  assert.equal((await first.closed).signal, 'SIGKILL');

  // The killed owner cannot release its lease. Recovery waits for database time to make it stale.
  await new Promise(resolve => setTimeout(resolve, 15500));
  const resumedOutput = await run(
    'java',
    [...java, 'batch-auto-resume', tenant, String(concurrency)],
    env,
    360000,
  );
  const recovered = JSON.parse(resumedOutput.match(/^BATCH_RECOVERED (.+)$/m)?.[1] || 'null');
  assert.equal(recovered.requested, concurrency);
  assert.equal(recovered.succeeded, concurrency);
  assert.equal(recovered.failed, 0);
  assert.ok(recovered.resources.heapGrowthBytes <= maxHeapGrowthBytes,
    `durable heap growth exceeded bound: ${JSON.stringify(recovered.resources)}`);
  assert.ok(recovered.resources.platformThreadGrowth <= maxPlatformThreadGrowth,
    `durable platform-thread growth exceeded bound: ${JSON.stringify(recovered.resources)}`);
  assert.ok(recovered.resources.nodePermitInFlightHighWater >= 1
    && recovered.resources.nodePermitInFlightHighWater <= maxNodePermitInFlight,
  `durable node permit high-water invalid: ${JSON.stringify(recovered.resources)}`);
  assert.ok(recovered.resources.storageCapacityInFlightHighWater >= 1
    && recovered.resources.storageCapacityInFlightHighWater <= maxStorageCapacityInFlight,
  `durable storage capacity high-water invalid: ${JSON.stringify(recovered.resources)}`);
  assert.equal(recovered.resources.nodePermitInFlightFinal, 0);
  assert.equal(recovered.resources.storageCapacityInFlightFinal, 0);
  assert.deepEqual(recovered.calls, [
    { calls: concurrency, node: 'first' },
    { calls: concurrency, node: 'second' },
    { calls: concurrency, node: 'third' },
  ]);
  console.log(JSON.stringify({
    kind: 'task14-durable-sigkill-capacity',
    concurrency,
    killedAt: 'first-business-checkpoint',
    recovered,
  }, null, 2));
} finally {
  for (const child of children) child.kill('SIGKILL');
  if (container) {
    const owner = await run(docker, ['inspect', '--format', '{{index .Config.Labels "flow-probe"}}', container]);
    assert.equal(owner, label, 'only remove this run-owned disposable container');
    await run(docker, ['rm', '-f', '-v', container]);
  }
}
