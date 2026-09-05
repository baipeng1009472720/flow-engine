import assert from 'node:assert/strict';

// Independent-process acceptance only against explicitly enabled loopback examples; no business data.
const base = new URL(process.argv[2] || 'http://127.0.0.1:18081');
assert.equal(base.hostname, '127.0.0.1');
assert.equal(base.protocol, 'http:');
async function request(path, { method = 'GET', data, headers = {} } = {}) {
  return fetch(new URL(path, base), {
    method, headers: { 'Content-Type': 'application/json', ...headers },
    body: data === undefined ? undefined : JSON.stringify(data), signal: AbortSignal.timeout(35000),
  });
}
async function json(path, options) {
  const response = await request(path, options);
  assert.ok(response.ok, `${path}: HTTP ${response.status} ${await (response.ok ? Promise.resolve('') : response.text())}`);
  return response.json();
}
function parseEvents(text) {
  return text.replaceAll('\r\n', '\n').split('\n\n').flatMap(frame => {
    const data = frame.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5)).join('\n');
    return data ? [JSON.parse(data)] : [];
  });
}
async function terminal(id) {
  for (let i = 0; i < 100; i++) {
    const execution = await json(`/v1/executions/${id}`);
    if (execution.finishedAt) return execution;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error(`execution did not finish: ${id}`);
}
const health = await json('/actuator/health');
assert.equal(health.status, 'UP');
const wait = await json('/v1/executions', { method: 'POST', data: { flowReference: 'demo:all', responseMode: 'WAIT' } });
assert.equal(wait.status, 'SUCCEEDED');
const expected = { results: [10, 20], child: { message: 'child-complete' }, selected: 'stream-complete', http: { message: 'demo-http' } };
assert.deepEqual(await json(`/v1/payloads/${wait.resultPayloadRef}`), expected);
const nodes = await json(`/v1/executions/${wait.executionId}/nodes?limit=500`);
assert.equal(new Set(nodes.map(node => node.componentType)).size, 11);
const parentEvents = await json(`/v1/executions/${wait.executionId}/events?limit=500`);
const childCreated = parentEvents.find(event => event.eventType === 'CHILD_CREATED');
assert.ok(childCreated, 'parent execution must expose child discovery');
const child = await json(`/v1/executions/${childCreated.attributes.childExecutionId}`);
assert.equal(child.rootExecutionId, wait.executionId);
assert.equal(child.parentExecutionId, wait.executionId);
assert.equal(child.status, 'SUCCEEDED');
const childNodes = await json(`/v1/executions/${child.executionId}/nodes`);
assert.ok(childNodes.every(node => node.scopePath.includes('subflow:sub')));
const bodies = nodes.filter(node => node.nodeCode === 'body');
assert.equal(bodies.length, 2);
assert.equal(new Set(bodies.map(node => node.scopePath)).size, 2);
for (const node of bodies) {
  const logs = await json(`/v1/node-executions/${node.nodeExecutionId}/logs`);
  const diagnostic = logs.find(log => log.maskedAttributes.diagnosticPayloadRef);
  assert.ok(diagnostic, 'Groovy println must have a node-bound protected diagnostic');
  const payload = await json(`/v1/payloads/${diagnostic.maskedAttributes.diagnosticPayloadRef}`);
  assert.ok(payload.message.includes('循环演示'));
}
const response = await request('/v1/executions/stream', { method: 'POST', data: { flowReference: 'demo:all' } });
assert.equal(response.status, 200);
assert.match(response.headers.get('content-type'), /text\/event-stream/);
const events = parseEvents(await response.text());
assert.ok(events.length > 0);
const streamId = events[0].executionId;
assert.ok(events.every(event => event.executionId === streamId));
assert.equal(new Set(events.map(event => event.eventId)).size, events.length);
assert.ok(events.every((event, index) => index === 0 || event.sequence > events[index - 1].sequence));
assert.equal(events.filter(event => !event.nodeId && event.eventType === 'COMPLETED').length, 1);
assert.equal(events.filter(event => event.eventType === 'CONTENT').length, 2);
assert.deepEqual(events.filter(event => event.eventType === 'CONTENT').map(event => event.attributes.payload), ['第一段', '第二段']);
assert.ok(events.find(event => event.eventType === 'CONTENT').sequence <
  events.find(event => event.eventType === 'NODE_COMPLETED' && event.nodeCode === 'stream').sequence);
assert.equal(events.at(-1).eventType, 'COMPLETED');
const streamed = await json(`/v1/executions/${streamId}`);
assert.deepEqual(await json(`/v1/payloads/${streamed.resultPayloadRef}`), expected);
const cursor = events[Math.floor(events.length / 2)];
const replay = await request(`/v1/executions/${streamId}/stream`, { headers: { 'Last-Event-ID': cursor.eventId } });
assert.equal(replay.status, 200);
assert.deepEqual(parseEvents(await replay.text()).map(event => event.eventId),
  events.filter(event => event.sequence > cursor.sequence).map(event => event.eventId));
assert.equal((await json(`/v1/executions/${streamId}`)).startedAt, streamed.startedAt);
const wrongCursor = await request(`/v1/executions/${streamId}/stream`, { headers: { 'Last-Event-ID': `${wait.executionId}:0` } });
assert.equal(wrongCursor.status, 400);

const slow = await json('/v1/executions', { method: 'POST', data: { flowReference: 'demo:slow', responseMode: 'ASYNC' } });
try {
  const stream = await request(`/v1/executions/${slow.executionId}/stream`);
  const reader = stream.body.getReader();
  await reader.read();
  await reader.cancel();
  const stillRunning = await json(`/v1/executions/${slow.executionId}`);
  assert.equal(stillRunning.status, 'RUNNING', 'disconnect must not cancel execution');
  assert.equal((await json(`/v1/executions/${slow.executionId}/cancel`, { method: 'POST' })).cancelRequested, true);
  assert.equal((await terminal(slow.executionId)).status, 'CANCELLED');
  assert.equal((await request(`/v1/executions/${slow.executionId}/resume`, { method: 'POST' })).status, 409);
} finally { await request(`/v1/executions/${slow.executionId}/cancel`, { method: 'POST' }); }
const failed = await json('/v1/executions', { method: 'POST', data: { flowReference: 'demo:failure' } });
assert.equal(failed.status, 'FAILED');
const failedNodes = await json(`/v1/executions/${failed.executionId}/nodes`);
const failedNode = failedNodes.find(node => node.nodeCode === 'invalid');
assert.equal(failedNode.errorId, failed.errorId);
const failedLogs = await json(`/v1/node-executions/${failedNode.nodeExecutionId}/logs`);
assert.ok(failedLogs.some(log => log.level === 'ERROR' && log.errorId === failed.errorId));
const failedEvents = await json(`/v1/executions/${failed.executionId}/events`);
assert.ok(failedEvents.some(event => event.eventType === 'FAILED' && event.attributes.errorId === failed.errorId));
console.log(JSON.stringify({ health: health.status, wait: wait.executionId, stream: streamId,
  nodeTypes: 11, loopIterations: bodies.length, streamEvents: events.length,
  contentChunks: 2, reconnect: 'same execution, no missing or duplicate events',
  disconnect: 'execution continues', cancel: 'CANCELLED', childDiscovery: 'passed', errorCorrelation: 'passed', output: expected }, null, 2));
