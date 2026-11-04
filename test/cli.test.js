import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, cp, symlink, writeFile, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const source = fileURLToPath(new URL('..', import.meta.url));
const preload = `
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
const file = process.env.PROBE_QUESTIONS;
const questions = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : {};
globalThis.fetch = async (raw, init) => {
  if (process.env.PROBE_OFFLINE === 'yes') throw new Error('Offline command attempted a request');
  const url = new URL(raw);
  assert.equal(url.origin, 'https://api.pingroom.io');
  assert.equal(init.redirect, 'manual');
  if (init.method === 'GET' && url.pathname === '/api/agent/rooms') return Response.json([{ invite_code: 'DEMO12345678', type: 'standard', is_public: false, members: [{ user_id: '11111111-1111-4111-8111-111111111111' }] }]);
  if (init.method === 'POST' && url.pathname === '/api/agent/rooms/DEMO12345678/questions') {
    const body = JSON.parse(init.body), now = new Date().toISOString();
    assert.equal(body.options, undefined);
    assert.deepEqual(body.text_input, {placeholder:'32 hex characters or Missing',max_length:32});
    assert.equal(new Headers(init.headers).get('idempotency-key'), body.correlation_id);
    const id = '01990000-0000-7000-8000-' + String(Object.keys(questions).length + 1).padStart(12, '0');
    const q = { ...body, id, kind: 'question', is_handoff: false, resolution_policy: 'first', room_code: 'DEMO12345678', room: { code: 'DEMO12345678' },
      asker: { type: 'agent', id: '33333333-3333-4333-8333-333333333333' }, results: null, options: [],
      state: 'pending', created_at: now, expires_at: new Date(Date.parse(now) + 900000).toISOString(), resolved_at: null, answer: null, reply_to: null };
    questions[id] = q; writeFileSync(file, JSON.stringify(questions));
    if (process.env.PROBE_LOST === 'yes') throw new Error('Lost creation response');
    return Response.json(q, { status: 201 });
  }
  if (init.method === 'GET' && url.pathname === '/api/agent/questions') return Response.json({questions:Object.values(questions)});
  if (init.method === 'GET' && url.pathname.startsWith('/api/agent/questions/')) {
    const q = questions[url.pathname.split('/').at(-1)];
    if (!q) throw new Error('Unexpected Question');
    if (process.env.PROBE_ANSWER !== undefined) {
      q.state = 'answered'; q.resolved_at = new Date().toISOString();
      q.answer = { value:null, label:null, text:process.env.PROBE_ANSWER, responder:{id:q.target_user_id}, answered_at:q.resolved_at };
    } else if (process.env.PROBE_CANCEL === 'yes') { q.state = 'cancelled'; q.resolved_at = new Date().toISOString(); }
    writeFileSync(file, JSON.stringify(questions)); return Response.json(q);
  }
  throw new Error('Unexpected mocked request');
};
`;
async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'parity-recovery-cli-test-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await cp(join(source, 'src'), join(directory, 'src'), { recursive: true });
  await cp(join(source, 'package.json'), join(directory, 'package.json'));
  await cp(join(source, 'config.example.json'), join(directory, 'config.json'));
  await symlink(join(source, 'node_modules'), join(directory, 'node_modules'));
  await writeFile(join(directory, 'preload.mjs'), preload);
  await cp(join(source, 'examples/packet.json'), join(directory, 'packet.json'));
  return directory;
}
function run(directory, command = ['run'], extra = {}) {
  const env = { ...process.env, PINGROOM_TOKEN: 'offline-fixture', PINGROOM_ROOM: 'DEMO12345678', PROBE_QUESTIONS: join(directory, 'questions.json') };
  for (const key of ['NODE_OPTIONS','PROBE_ANSWER','PROBE_CANCEL','PROBE_LOST','PROBE_OFFLINE']) delete env[key];
  Object.assign(env,extra);
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', join(directory, 'preload.mjs'), join(directory, 'src/cli.js'), ...command], { cwd: tmpdir(), env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', data => { stdout += data; }); child.stderr.on('data', data => { stderr += data; });
    const timer = setTimeout(() => child.kill('SIGKILL'), 8000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('close', code => { clearTimeout(timer); resolve({ code, stdout, stderr }); });
  });
}
async function collect(directory, values) {
  let answer;
  for(const [i,value] of values.entries()) {
    const pending=await run(directory);assert.equal(pending.code,3,pending.stderr);assert.equal(JSON.parse(pending.stdout).status,'pending');
    answer=await run(directory,['run'],{PROBE_ANSWER:value});
    if(i<values.length-1)assert.ok([2,3].includes(answer.code),answer.stderr);
  }
  return answer;
}


const cards = JSON.parse(await readFile(join(source, 'examples/cards.json'), 'utf8'));
const values = cards.shards.map(s => s.hex);

test('CLI recovers one data erasure, preserves attribution and exports committed private bytes offline', async t => {
  const directory = await fixture(t), original = await readFile(join(directory, 'packet.json'));
  const preview = await run(directory, ['preview'], { PROBE_OFFLINE: 'yes' }); assert.equal(preview.code, 0, preview.stderr); assert.equal(JSON.parse(preview.stdout).shards.length, 3);
  const finished = await collect(directory, [values[0], 'Missing', values[2]]); assert.equal(finished.code, 0, finished.stderr);
  const report = JSON.parse(finished.stdout); assert.equal(report.packet.recoveredShardId, 'd2'); assert.equal(report.packet.parityStatus, 'verified');
  for (const command of ['status', 'report']) {
    const result = await run(directory, [command], { PROBE_OFFLINE: 'yes' }); assert.equal(result.code, 0, result.stderr); assert.deepEqual(JSON.parse(result.stdout).packet, report.packet);
  }
  const exported = await run(directory, ['export'], { PROBE_OFFLINE: 'yes', PINGROOM_TOKEN: undefined, PINGROOM_ROOM: undefined }); assert.equal(exported.code, 0, exported.stderr);
  const output = JSON.parse(exported.stdout); assert.equal(output.committed, true);
  assert.equal((await readFile(output.path)).toString('hex'), report.packet.payloadHex); assert.equal((await stat(output.path)).mode & 0o777, 0o600);
  const provenance = JSON.parse(await readFile(output.provenancePath, 'utf8')); assert.deepEqual(provenance.answers, report.answers);
  assert.equal((await run(directory, ['export'], { PROBE_OFFLINE: 'yes' })).code, 0); assert.deepEqual(await readFile(join(directory, 'packet.json')), original);
  assert.equal(Object.keys(JSON.parse(await readFile(join(directory, 'questions.json'), 'utf8'))).length, 3);
});

test('CLI all-data without parity succeeds explicitly; insufficient or malformed transcriptions refuse export', async t => {
  const complete = await fixture(t), done = await collect(complete, [values[0], values[1], 'Missing']); assert.equal(done.code, 0, done.stderr);
  assert.equal(JSON.parse(done.stdout).packet.parityStatus, 'unavailable');
  for (const sequence of [['Missing', 'Missing', values[2]], ['Missing', values[1], 'Missing'], ['gg'.repeat(16), values[1], values[2]], ['ff'.repeat(16), values[1], values[2]]]) {
    const directory = await fixture(t); assert.equal((await run(directory, ['export'], { PROBE_OFFLINE: 'yes' })).code, 1);
    const finished = await collect(directory, sequence); assert.equal(finished.code, 2, finished.stderr); assert.equal(JSON.parse(finished.stdout).packet, null);
    assert.equal((await run(directory, ['export'], { PROBE_OFFLINE: 'yes' })).code, 1); await assert.rejects(readFile(join(directory, '.parity/complete.json')), { code: 'ENOENT' });
    assert.equal(Object.keys(JSON.parse(await readFile(join(directory, 'questions.json'), 'utf8'))).length, 3);
  }
});

test('CLI uncertain creation recovers the exact original Question without another POST', async t => {
  const directory = await fixture(t);
  assert.equal((await run(directory, ['run'], { PROBE_LOST: 'yes' })).code, 4); assert.equal((await run(directory)).code, 4);
  const found = await run(directory, ['find']); assert.equal(found.code, 4, found.stderr); const id = JSON.parse(found.stdout).candidates[0].id;
  const recovered = await run(directory, ['recover', id], { PROBE_ANSWER: values[0] }); assert.equal(recovered.code, 3, recovered.stderr);
  assert.equal(JSON.parse(recovered.stdout).answers[0].text, values[0]); assert.equal(Object.keys(JSON.parse(await readFile(join(directory, 'questions.json'), 'utf8'))).length, 1);
  assert.equal((await run(directory, ['recover', id + '\n'])).code, 1);
});

test('CLI cancellation and oversized text fail while later shard collection remains possible', async t => {
  for (const extra of [{ PROBE_CANCEL: 'yes' }, { PROBE_ANSWER: '0'.repeat(33) }]) {
    const directory = await fixture(t); await run(directory); const result = await run(directory, ['run'], extra);
    assert.equal(result.code, 2, result.stderr); assert.equal(JSON.parse(result.stdout).status, 'ready'); assert.equal(JSON.parse(result.stdout).packet, null);
    assert.equal((await run(directory, ['export'], { PROBE_OFFLINE: 'yes' })).code, 1);
  }
});

test('CLI rejects partial credentials, newline tokens and arbitrary output paths; complete dotenv setup works', async t => {
  const directory = await fixture(t); await writeFile(join(directory, '.env'), 'PINGROOM_TOKEN=offline-fixture\nPINGROOM_ROOM=DEMO12345678\n');
  for (const extra of [{ PINGROOM_TOKEN: '' }, { PINGROOM_ROOM: '' }, { PINGROOM_TOKEN: 'offline\n' }, { PINGROOM_ROOM: 'DEMO12345678\n' }, { PINGROOM_ROOM: undefined }]) {
    const result = await run(directory, ['run'], extra); assert.equal(result.code, 1); assert.equal(result.stdout, '');
  }
  await assert.rejects(readFile(join(directory, 'questions.json')), { code: 'ENOENT' });
  assert.equal((await run(directory, ['run'], { PINGROOM_TOKEN: undefined, PINGROOM_ROOM: undefined })).code, 3);
  for (const command of ['export', 'prepare']) assert.equal((await run(directory, [command, '/tmp/not-allowed.bin'], { PROBE_OFFLINE: 'yes' })).code, 1);
});

test('CLI prepare makes reusable cards and manifest from fixed binary input, refuses conflicting reruns and supports copying prepared manifest into collection', async t => {
  const directory = await fixture(t), metadata = { packetId: 'custom', title: 'Custom label', context: 'Non-secret bytes' }, payload = Buffer.from([0, 255, 10, 13, 200]);
  await writeFile(join(directory, 'packet-meta.json'), JSON.stringify(metadata)); await writeFile(join(directory, 'payload.bin'), payload);
  const prepared = await run(directory, ['prepare'], { PROBE_OFFLINE: 'yes' }); assert.equal(prepared.code, 0, prepared.stderr);
  const output = JSON.parse(prepared.stdout); assert.equal(output.committed, true); assert.equal((await run(directory, ['prepare'], { PROBE_OFFLINE: 'yes' })).code, 0);
  const preparedCards = JSON.parse(await readFile(output.cardsPath, 'utf8')); await cp(output.manifestPath, join(directory, 'packet.json'));
  const finished = await collect(directory, ['Missing', preparedCards.shards[1].hex]); assert.equal(finished.code, 0, finished.stderr);
  const exported = await run(directory, ['export'], { PROBE_OFFLINE: 'yes' }); assert.equal(exported.code, 0, exported.stderr); assert.deepEqual(await readFile(JSON.parse(exported.stdout).path), payload);
  await writeFile(join(directory, 'payload.bin'), Buffer.alloc(97)); assert.equal((await run(directory, ['prepare'], { PROBE_OFFLINE: 'yes' })).code, 1);
  await writeFile(join(directory, 'payload.bin'), Buffer.from('changed')); assert.equal((await run(directory, ['prepare'], { PROBE_OFFLINE: 'yes' })).code, 1);
  assert.deepEqual(JSON.parse(await readFile(output.cardsPath, 'utf8')), preparedCards);
});

test('CLI changed manifest binding is rejected before new requests', async t => {
  const directory = await fixture(t); await run(directory); const original = await readFile(join(directory, 'questions.json'));
  const input = JSON.parse(await readFile(join(directory, 'packet.json'), 'utf8')); input.context += ' changed'; await writeFile(join(directory, 'packet.json'), JSON.stringify(input));
  assert.equal((await run(directory, ['run'], { PROBE_OFFLINE: 'yes' })).code, 1); assert.deepEqual(await readFile(join(directory, 'questions.json')), original);
});
