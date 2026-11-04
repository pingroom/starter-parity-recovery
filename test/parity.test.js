import { prepareProject } from '../src/prepare.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, writeFile, stat, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { step, localReport, exportPacket } from '../src/engine.js';
import { readJson, readBytes, atomicJson, acquireLock, atomicText, writeOnce } from '../src/storage.js';
import { validateInput, validateState, itemsFor, parseShard, packPacket, recoverPacket, digest, requestFor, validateConfig, hash, iso, isId, ROOM, TOKEN } from '../src/model.js';
import { loadConnection, loadEnvironment, validateConnection } from '../src/config.js';
import { makeClient } from '../src/client.js';
import { mockApi, CONFIG, ROOM_CODE, OTHER, ASKER, id } from '../src/mock.js';

const example = await readJson(new URL('../examples/packet.json', import.meta.url), 65536);
const simple = () => structuredClone(example);
const twoItems = () => structuredClone(example);
async function setup(t, input = simple()) {
  const directory = await mkdtemp(join(tmpdir(), 'pingroom-parity-test-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const clock = { value: '2026-01-01T09:00:00.000Z' }, now = () => clock.value;
  const api = mockApi(now);
  const options = { client: api.client, directory, input, config: structuredClone(CONFIG), token: 'offline-fixture-token', room: ROOM_CODE, now };
  return { directory, clock, api, options, run: (extra = {}) => step({ ...options, ...extra }), saved: () => readJson(join(directory, 'state.json')) };
}
const sends = (api) => api.calls.filter((call) => call.method === 'POST').length;
async function decide(s, decisions) {
  let result;
  for (const value of decisions) {
    result = await s.run();
    const pending = result.answers.find(item => item.status === 'pending');
    s.api.answer(pending.questionId, value); result = await s.run();
  }
  return result;
}

test('lost response blocks later shards; exact-ID recovery reads the original request without resend', async (t) => {
  const s = await setup(t, twoItems()); s.api.controls.loseNext = true;
  assert.equal((await s.run()).status, 'create_unknown'); assert.equal((await s.run()).status, 'create_unknown');
  assert.deepEqual((await s.run({ command: 'find' })).candidates, [{ id: id(1), state: 'pending' }]);
  s.api.answer(id(1), 'Missing'); assert.equal((await s.run({ command: 'recover', attachId: id(1) })).status, 'ready');
  assert.equal(sends(s.api), 1);
});

test('malformed create response and empty search never authorize a new POST', async (t) => {
  const s = await setup(t); s.api.controls.malformedNext = true; await assert.rejects(s.run());
  assert.equal((await s.saved()).attempts[0].status, 'creating'); s.api.questions.clear();
  assert.deepEqual((await s.run({ command: 'find' })).candidates, []);
  assert.equal((await s.run()).status, 'create_unknown'); assert.equal(sends(s.api), 1);
});

test('recovery validates exact target, room, request, options, data and non-handoff provenance', async (t) => {
  for (const mutate of [
    (q) => { q.target_user_id = OTHER; }, (q) => { q.room.code = 'WRONG1234567'; }, (q) => { q.prompt = 'Other prompt'; },
    (q) => { q.options = [{value:'yes',label:'Yes'}]; }, (q) => { q.text_input.max_length = 30; }, (q) => { q.text_input = null; },
    (q) => { q.data.item_id = 'other'; }, (q) => { q.data.input_hash = 'changed'; }, (q) => { delete q.is_handoff; },
    (q) => { q.is_handoff = true; }, (q) => { q.asker.type = 'user'; }, (q) => { q.resolution_policy = 'all'; },
  ]) {
    const s = await setup(t); s.api.controls.loseNext = true; await s.run(); mutate(s.api.questions.get(id(1)));
    await assert.rejects(s.run({ command: 'recover', attachId: id(1) })); assert.equal((await s.saved()).attempts[0].status, 'creating');
    assert.equal(sends(s.api), 1);
  }
});

test('misattributed, malformed or non-text answers never resolve a transcription', async (t) => {
  for (const mutate of [
    (q) => { q.answer.responder.id = OTHER; }, (q) => { q.answer.responder = null; },
    (q) => { q.answer.text = null; }, (q) => { q.answer.value = 'unsupported'; }, (q) => { q.answer.label = 'Wrong'; },
  ]) {
    const s = await setup(t); await s.run(); s.api.answer(id(1), 'Missing'); mutate(s.api.questions.get(id(1)));
    const result = await s.run(); assert.equal(result.observedStatus, 'invalid_answer'); assert.equal(result.packet, null);
    assert.equal(sends(s.api), 1);
  }
});

test('original asker, lifetime and intent remain bound across reads and across shards', async (t) => {
  for (const mutate of [
    (q) => { q.asker.id = OTHER; },
    (q) => { q.created_at = '2026-01-01T09:00:01.000Z'; q.expires_at = '2026-01-01T09:15:01.000Z'; },
  ]) {
    const s = await setup(t); await s.run(); mutate(s.api.questions.get(id(1))); await assert.rejects(s.run(), /identity or lifetime/);
  }
  const s = await setup(t, twoItems()); await s.run(); s.api.answer(id(1), 'Missing'); await s.run();
  s.api.controls.loseNext = true; await s.run(); s.api.questions.get(id(2)).asker.id = OTHER;
  await assert.rejects(s.run({ command: 'recover', attachId: id(2) }), /different asking registrations/);
  const other = await setup(t); other.api.controls.loseNext = true; await other.run();
  const q = other.api.questions.get(id(1)); q.created_at = '2026-01-01T08:58:59.000Z'; q.expires_at = '2026-01-01T09:13:59.000Z';
  await assert.rejects(other.run({ command: 'recover', attachId: id(1) }), /predates/);
});

test('expiry accepts delayed processing but rejects premature and implausibly future resolution on replay', async (t) => {
  const s = await setup(t); await s.run(); s.clock.value = '2026-01-01T09:16:00.000Z';
  s.api.terminal(id(1), 'expired'); s.api.questions.get(id(1)).resolved_at = '2026-01-01T09:15:30.000Z';
  assert.equal((await s.run()).observedStatus, 'expired'); assert.equal((await localReport(s.directory, s.options.input)).packet, null);
  const original = await s.saved();
  for (const timestamp of ['2026-01-01T09:14:59.999Z', '2026-01-01T09:17:00.001Z']) {
    const saved = structuredClone(original); saved.attempts[0].observation.resolvedAt = timestamp;
    await atomicJson(s.directory, 'state.json', saved); const calls = s.api.calls.length;
    await assert.rejects(s.run(), /resolution time/); assert.equal(s.api.calls.length, calls);
    const live = await setup(t); await live.run(); live.clock.value = s.clock.value; live.api.terminal(id(1), 'expired');
    live.api.questions.get(id(1)).resolved_at = timestamp; await assert.rejects(live.run(), /resolution time/);
  }
});

test('answer and resolution times must agree and answer/cancellation cannot occur after expiry', async (t) => {
  const s = await setup(t); await s.run(); s.api.answer(id(1), 'Missing');
  s.api.questions.get(id(1)).answer.answered_at = '2026-01-01T09:00:01.000Z';
  assert.equal((await s.run()).observedStatus, 'invalid_answer');
  for (const terminal of ['answered', 'cancelled']) {
    const other = await setup(t); await other.run(); other.clock.value = '2026-01-01T09:16:00.000Z';
    if (terminal === 'answered') other.api.answer(id(1), 'Missing'); else other.api.terminal(id(1), terminal);
    await assert.rejects(other.run(), /resolution time/);
  }
});

test('checked time follows response; future creation, wrong TTL and answer on pending fail', async (t) => {
  const s = await setup(t); await s.run(); const get = s.api.client.questions.get.bind(s.api.client.questions);
  s.api.client.questions.get = async (...args) => { s.clock.value = '2026-01-01T09:02:00.000Z'; s.api.answer(id(1), 'Missing'); return get(...args); };
  await s.run(); assert.equal((await s.saved()).attempts[0].observation.checkedAt, s.clock.value);
  for (const mutate of [
    (q) => { q.created_at = '2027-01-01T09:00:00.000Z'; q.expires_at = '2027-01-01T09:15:00.000Z'; },
    (q) => { q.expires_at = '2026-01-01T10:00:00.000Z'; }, (q) => { q.answer = {}; },
  ]) { const other = await setup(t); await other.run(); mutate(other.api.questions.get(id(1))); await assert.rejects(other.run()); }
  assert.equal(iso('2026-01-01T09:00:00+00:00'), '2026-01-01T09:00:00.000Z'); assert.throws(() => iso('2026-02-30T09:00:00Z'));
});

test('room preflight refuses public/unsupported rooms and missing operator before a POST', async (t) => {
  for (const mutate of [(rooms) => { rooms[0].is_public = true; }, (rooms) => { rooms[0].type = 'dm'; }, (rooms) => { rooms[0].members = [{ user_id: OTHER }]; }]) {
    const s = await setup(t); mutate(s.api.rooms); await assert.rejects(s.run()); assert.equal(sends(s.api), 0);
  }
});

test('reservation write failure sends nothing; answer save failure resumes the original question', async (t) => {
  const s = await setup(t); let writes = 0;
  await assert.rejects(s.run({ write: async (...args) => { if (++writes === 2) throw new Error('disk full'); await atomicJson(...args); } }), /disk full/);
  assert.equal(sends(s.api), 0); await s.run(); s.api.answer(id(1), 'Missing');
  await assert.rejects(s.run({ write: async () => { throw new Error('disk full'); } }), /disk full/);
  assert.equal((await s.saved()).attempts[0].status, 'pending'); assert.equal((await s.run()).status, 'ready'); assert.equal(sends(s.api), 1);
});

test('post-create save failure leaves uncertain intent for verified recovery without another POST', async (t) => {
  const s = await setup(t); let writes = 0;
  await assert.rejects(s.run({ write: async (...args) => { if (++writes === 3) throw new Error('disk full'); await atomicJson(...args); } }), /disk full/);
  assert.equal((await s.run()).status, 'create_unknown');
  assert.equal((await s.run({ command: 'recover', attachId: id(1) })).status, 'pending'); assert.equal(sends(s.api), 1);
});

test('failed reads preserve evidence and corrupted request/provenance fail before network', async (t) => {
  const s = await setup(t); await s.run(); const before = await readFile(join(s.directory, 'state.json'), 'utf8');
  s.api.controls.readError = true; await assert.rejects(s.run()); assert.equal(await readFile(join(s.directory, 'state.json'), 'utf8'), before);
  s.api.controls.readError = false; s.api.answer(id(1), 'Missing'); await s.run(); const original = await s.saved();
  for (const mutate of [
    (saved) => { saved.attempts[0].request.ttl = 60; }, (saved) => { saved.attempts.push(saved.attempts[0]); },
    (saved) => { saved.attempts[0].pair = 'other::pair'; }, (saved) => { saved.attempts[0].observation.answer.responderId = OTHER; },
  ]) {
    const saved = structuredClone(original); mutate(saved); assert.throws(() => validateState(saved, s.options.input));
    await atomicJson(s.directory, 'state.json', saved); const calls = s.api.calls.length;
    await assert.rejects(s.run()); assert.equal(s.api.calls.length, calls);
  }
});

test('bounded file reads reject malformed, oversized, invalid UTF-8 and symlink data without revealing contents', async (t) => {
  const s = await setup(t), path = join(s.directory, 'bad.json');
  await writeFile(path, '{"secret":"PRIVATE_TOKEN",bad');
  await assert.rejects(readJson(path), (error) => !error.message.includes('PRIVATE_TOKEN') && error.message.includes('malformed'));
  await writeFile(path, Buffer.from([0xff])); await assert.rejects(readJson(path), /UTF-8/);
  await writeFile(path, 'x'.repeat(100)); await assert.rejects(readJson(path, 50), /size limit/);
  await symlink(path, join(s.directory, 'linked')); await assert.rejects(readJson(join(s.directory, 'linked')));
});

test('private lock excludes other commands; report is offline and private', async (t) => {
  const s = await setup(t); await s.run(); const release = await acquireLock(s.directory);
  await assert.rejects(s.run(), /lock exists/); await assert.rejects(localReport(s.directory, s.options.input), /lock exists/);
  await release(); const before = s.api.calls.length;
  assert.equal((await localReport(s.directory, s.options.input, { save: true })).packet, null); assert.equal(s.api.calls.length, before);
  assert.equal((await stat(join(s.directory, 'report.json'))).mode & 0o777, 0o600);
});

test('partial or blank environment overrides never combine with saved credentials', async (t) => {
  const s = await setup(t); await atomicJson(s.directory, 'credentials.json', { token: 'saved', room: ROOM_CODE });
  await assert.rejects(loadConnection(s.directory, { PINGROOM_TOKEN: '' })); await assert.rejects(loadConnection(s.directory, { PINGROOM_ROOM: ROOM_CODE }));
  assert.equal((await loadConnection(s.directory, { PINGROOM_TOKEN: 'explicit', PINGROOM_ROOM: ROOM_CODE.toLowerCase() })).token, 'explicit');
});

test('transport rejects unrelated writes, redirects and oversized responses', async () => {
  let count = 0; const client = makeClient({ token: 'fixture', fetchImpl: async () => { count++; return new Response(null, { status: 302 }); } });
  await assert.rejects(client.questions.get(id(1))); assert.equal(count, 1); await assert.rejects(client.questions.cancel(id(1))); assert.equal(count, 1);
  const large = makeClient({ token: 'fixture', fetchImpl: async () => new Response('x'.repeat(512 * 1024 + 1)) }); await assert.rejects(large.questions.get(id(1)));
});

test('transport deadlines bound stalled fetch/body and allow abort through infinite ready empty chunks', async () => {
  const stalled = makeClient({ token: 'fixture', deadlineMs: 20, fetchImpl: () => new Promise(() => {}) }); await assert.rejects(stalled.questions.get(id(1)));
  const body = makeClient({ token: 'fixture', deadlineMs: 20, fetchImpl: async () => new Response(new ReadableStream({ pull: () => new Promise(() => {}) })) }); await assert.rejects(body.questions.get(id(1)));
  const controller = new AbortController();
  const empty = makeClient({ token: 'fixture', signal: controller.signal, fetchImpl: async () => new Response(new ReadableStream({ pull: (out) => out.enqueue(new Uint8Array()) })) });
  const timer = setTimeout(() => controller.abort(), 20); try { await assert.rejects(empty.questions.get(id(1))); } finally { clearTimeout(timer); }
});

test('abort before reservation sends nothing; after reservation it preserves unresolved intent', async (t) => {
  const s = await setup(t), stopped = new AbortController(); stopped.abort(); await assert.rejects(s.run({ signal: stopped.signal })); assert.equal(sends(s.api), 0);
  const controller = new AbortController(); let writes = 0;
  await assert.rejects(s.run({ signal: controller.signal, write: async (...args) => { await atomicJson(...args); if (++writes === 2) controller.abort(); } }));
  assert.equal(sends(s.api), 0); assert.equal((await s.run()).status, 'create_unknown');
});


test('partial external credentials cannot borrow the room from dotenv', async (t) => {
  const s = await setup(t), path = join(s.directory, '.env');
  await writeFile(path, 'PINGROOM_TOKEN=file-token\nPINGROOM_ROOM=DEMO12345678\n');
  const partial = { PINGROOM_TOKEN: 'shell-token' }; await loadEnvironment(partial, path);
  assert.equal(partial.PINGROOM_ROOM, undefined); await assert.rejects(loadConnection(s.directory, partial));
  const env = {}; await loadEnvironment(env, path); assert.deepEqual(await loadConnection(s.directory, env), { token: 'file-token', room: ROOM_CODE });
});



const cards = await readJson(new URL('../examples/cards.json', import.meta.url));
const cardTexts = () => Object.fromEntries(cards.shards.map(s => [s.id, s.hex]));
const metadata = { packetId: 'practice', title: 'Practice packet', context: 'Non-secret fixture' };
const texts = bundle => Object.fromEntries(bundle.cards.shards.map(s => [s.id, s.hex]));

// A separate BigInt parity oracle, with no call to the implementation's packing or XOR functions.
function oracle(payload) {
  const chunks = [];
  for (let i = 0; i < payload.length; i += 16) chunks.push(Buffer.concat([payload.subarray(i, i + 16), Buffer.alloc(Math.max(0, 16 - (payload.length - i)))]));
  const parity = chunks.reduce((value, chunk) => value ^ BigInt(`0x${chunk.toString('hex')}`), 0n).toString(16).padStart(32, '0');
  const values = [...chunks.map(c => c.toString('hex')), parity];
  return { manifest: { ...metadata, dataLength: payload.length, shardBytes: 16, shards: values.map((hex, i) => ({ id: i === chunks.length ? 'p' : `d${i + 1}`, sha256: digest(Buffer.from(hex, 'hex')) })), payloadSha256: digest(payload) },
    cards: { shards: values.map((hex, i) => ({ id: i === chunks.length ? 'p' : `d${i + 1}`, hex })) } };
}

test('independent BigInt parity oracle covers every payload length, every data erasure position and deterministic binary bytes', () => {
  let cases = 0;
  for (let length = 1; length <= 96; length++) {
    const payload = Buffer.from(Array.from({ length }, (_, i) => (i * 197 + length * 31) % 256)), bundle = oracle(payload);
    const packed = packPacket(metadata, payload); assert.deepEqual(packed.manifest, bundle.manifest); assert.deepEqual(packed.cards.shards, bundle.cards.shards);
    for (const erased of bundle.cards.shards.map(s => s.id)) {
      const supplied = texts(bundle); supplied[erased] = 'Missing'; const result = recoverPacket(bundle.manifest, supplied);
      assert.equal(result.status, 'complete'); assert.equal(result.packet.payloadHex, payload.toString('hex'));
      assert.equal(result.packet.recoveredShardId, erased === 'p' ? null : erased); assert.equal(result.packet.parityStatus, erased === 'p' ? 'unavailable' : 'verified'); cases++;
    }
  }
  assert.equal(cases, 432);
});

test('zero-filled and arbitrary binary payloads preserve exact length without text parsing', () => {
  for (const payload of [Buffer.alloc(1), Buffer.alloc(16), Buffer.alloc(17), Buffer.alloc(96), Buffer.from([0, 255, 192, 128, 0, 10, 13, 255])]) {
    const bundle = packPacket(metadata, payload), supplied = texts(bundle); supplied.d1 = 'Missing';
    const result = recoverPacket(bundle.manifest, supplied); assert.equal(result.status, 'complete'); assert.deepEqual(Buffer.from(result.packet.payloadHex, 'hex'), payload);
    assert.equal(result.packet.dataLength, payload.length);
  }
});

test('strict normalized text accepts upper/lowercase hex and exact Missing while malformed input is diagnostic evidence', () => {
  assert.deepEqual(parseShard('AB'.repeat(16)), { kind: 'hex', hex: 'ab'.repeat(16) });
  assert.deepEqual(parseShard('0'.repeat(32)), { kind: 'hex', hex: '0'.repeat(32) });
  assert.deepEqual(parseShard('Missing'), { kind: 'missing', hex: null });
  for (const bad of ['', 'missing', 'MISSING', 'Unknown', '0x' + '0'.repeat(30), 'g'.repeat(32), 'ab cd', '0'.repeat(31), 'Missing\n', ' Missing', '0'.repeat(30) + '\n', '\u200b']) assert.equal(parseShard(bad).kind, 'invalid');
  for (const bad of [null, 1, '0'.repeat(33), '\ud800']) assert.throws(() => parseShard(bad));
  const supplied = cardTexts(); supplied.d1 = supplied.d1.toUpperCase(); assert.equal(recoverPacket(simple(), supplied).status, 'complete');
});

test('real SDK recovery retains attributed transcription, validates every response and exports private exact bytes', async t => {
  const s = await setup(t), values = cardTexts(); values.d2 = 'Missing';
  const result = await decide(s, Object.values(values)); assert.equal(result.status, 'complete'); assert.equal(result.packet.recoveredShardId, 'd2');
  assert.ok(result.answers.every(a => a.responderId === CONFIG.participant.id && a.askerId === ASKER && a.answeredAt));
  const output = await exportPacket(s.directory, s.options.input); assert.equal(output.committed, true);
  assert.equal(digest(await readBytes(output.path)), simple().payloadSha256); assert.equal((await stat(output.path)).mode & 0o777, 0o600);
  const provenance = await readJson(output.provenancePath), { observedStatus, ...expected } = result;
  assert.deepEqual(provenance, expected); assert.equal(provenance.packet.bytesExecuted, false);
  const before = s.api.calls.length; await exportPacket(s.directory, s.options.input); await s.run(); assert.equal(s.api.calls.length, before); assert.equal(sends(s.api), 3);
});

test('all data without parity is valid and explicitly reports parity unavailable; unrecoverable erasures never become zero', async t => {
  const allData = cardTexts(); allData.p = 'Missing'; const done = recoverPacket(simple(), allData);
  assert.equal(done.status, 'complete'); assert.equal(done.packet.parityStatus, 'unavailable'); assert.equal(done.packet.recoveredShardId, null);
  for (const missing of [['d1', 'd2'], ['d1', 'p'], ['d1', 'd2', 'p']]) {
    const values = cardTexts(); for (const id of missing) values[id] = 'Missing';
    const s = await setup(t), result = await decide(s, Object.values(values)); assert.equal(result.status, 'unrecoverable'); assert.equal(result.packet, null);
    assert.equal(result.diagnostics[0].code, 'insufficient_shards'); assert.equal(sends(s.api), 3); await assert.rejects(exportPacket(s.directory, simple()));
  }
});

test('unresolved, cancelled or invalid identity responses block recovery even if the remaining bytes would suffice', async t => {
  const values = cardTexts(); values.p = null; const partial = recoverPacket(simple(), values);
  assert.equal(partial.status, 'incomplete'); assert.equal(partial.packet, null); assert.deepEqual(partial.unresolvedShardIds, ['p']);
  const s = await setup(t); await s.run(); s.api.terminal(id(1), 'cancelled'); await s.run();
  const done = await decide(s, [cards.shards[1].hex, cards.shards[2].hex]); assert.equal(done.status, 'incomplete'); assert.equal(done.packet, null);
  assert.equal(done.answers[0].status, 'cancelled'); await assert.rejects(exportPacket(s.directory, simple()));
});

test('malformed and checksum-mismatched observations are retained and never silently converted to erasures', async t => {
  for (const bad of ['g'.repeat(32), 'Missing\n', 'ff'.repeat(16)]) {
    const s = await setup(t), result = await decide(s, [bad, 'Missing', cards.shards[2].hex]);
    assert.equal(result.status, 'invalid'); assert.equal(result.packet, null); assert.equal(result.answers[0].text, bad); assert.equal(result.answers[0].status, 'answered');
    assert.ok(result.diagnostics.some(d => d.shardId === 'd1')); assert.deepEqual(result.missingShardIds, ['d2']); await assert.rejects(exportPacket(s.directory, simple()));
  }
});

test('padding, parity, reconstructed-shard and payload mismatches are independently rejected even with matching supplied shard hashes', () => {
  const base = oracle(Buffer.from([1, 2, 3, 4, 5]));
  const padded = structuredClone(base); const raw = Buffer.from(padded.cards.shards[0].hex, 'hex'); raw[15] = 7;
  for (const shard of padded.cards.shards) shard.hex = raw.toString('hex');
  for (const shard of padded.manifest.shards) shard.sha256 = digest(raw);
  assert.ok(recoverPacket(padded.manifest, texts(padded)).diagnostics.some(d => d.code === 'nonzero_padding'));
  const inconsistent = structuredClone(base); inconsistent.cards.shards.at(-1).hex = 'ff'.repeat(16); inconsistent.manifest.shards.at(-1).sha256 = digest(Buffer.alloc(16, 255));
  assert.ok(recoverPacket(inconsistent.manifest, texts(inconsistent)).diagnostics.some(d => d.code === 'inconsistent_parity'));
  const wrongReconstructed = structuredClone(base); wrongReconstructed.manifest.shards[0].sha256 = '0'.repeat(64); const supplied = texts(wrongReconstructed); supplied.d1 = 'Missing';
  assert.ok(recoverPacket(wrongReconstructed.manifest, supplied).diagnostics.some(d => d.code === 'reconstructed_checksum_mismatch'));
  const wrongPayload = structuredClone(base); wrongPayload.manifest.payloadSha256 = '0'.repeat(64);
  assert.ok(recoverPacket(wrongPayload.manifest, texts(wrongPayload)).diagnostics.some(d => d.code === 'payload_checksum_mismatch'));
});

test('immutable manifest and shard sequence reject changed metadata or ordering before network', async t => {
  const s = await setup(t); await s.run(); const before = s.api.calls.length;
  for (const mutate of [x => { x.context += ' Changed'; }, x => { x.title += ' Changed'; }, x => { x.payloadSha256 = '0'.repeat(64); }, x => { x.shards[0].sha256 = '0'.repeat(64); }]) {
    const input = simple(); mutate(input); await assert.rejects(s.run({ input })); assert.equal(s.api.calls.length, before);
  }
  const input = simple(); input.shards.reverse(); assert.throws(() => validateInput(input));
});

test('manifest schemas, digest grammar, exact key sets and Unicode limits reject invalid inputs before requests', async t => {
  for (const mutate of [
    x => { x.dataLength = 0; }, x => { x.dataLength = 97; }, x => { x.dataLength = 1.5; }, x => { x.dataLength = -0; }, x => { x.shardBytes = 32; },
    x => { x.shards.pop(); }, x => { x.shards[0].id = 'p'; }, x => { x.shards[1].id = 'd1'; }, x => { x.shards[0].sha256 = 'A'.repeat(64); },
    x => { x.payloadSha256 += '\n'; }, x => { x.shards[0].sha256 += '\n'; }, x => { x.packetId += '\n'; }, x => { x.shards[0].id += '\n'; },
    x => { x.title = 'x'.repeat(41); }, x => { x.context = 'x'.repeat(81); }, x => { x.context = '\ud800'; }, x => { x.title = 'x\u200by'; }, x => { x.context = ' trailing '; },
    x => { x['context,title'] = 'merged'; delete x.context; delete x.title; }, x => { x.shards[0] = { 'id,sha256': 'merged' }; },
  ]) { const input = simple(); mutate(input); const s = await setup(t, input); await assert.rejects(s.run()); assert.equal(s.api.calls.length, 0); }
  assert.throws(() => recoverPacket(simple(), { 'd1,d2,p': 'Missing' }));
  assert.throws(() => packPacket(metadata, Buffer.alloc(0))); assert.throws(() => packPacket(metadata, Buffer.alloc(97)));
});

test('maximum packet creates seven bounded prompts without including expected contents', async t => {
  const bundle = packPacket({ packetId: 'p'.repeat(40), title: '🙂'.repeat(20), context: '界'.repeat(80) }, Buffer.alloc(96, 255));
  for (const shard of bundle.manifest.shards) {
    const request = requestFor(bundle.manifest, CONFIG, shard.id, id(1));
    assert.ok(request.prompt.length <= 500); assert.ok(request.context.length <= 40); assert.equal(request.text_input.max_length, 32); assert.equal(request.options, undefined);
    assert.ok(request.prompt.includes(shard.id)); assert.ok(request.prompt.includes(bundle.manifest.context)); assert.ok(!request.prompt.includes('ff'.repeat(16)));
  }
  const s = await setup(t, bundle.manifest), result = await decide(s, Object.values(texts(bundle))); assert.equal(result.status, 'complete'); assert.equal(sends(s.api), 7);
});

test('central token, room, UUID and path boundaries reject trailing newlines before requests or writes', async t => {
  assert.equal(TOKEN.test('offline\n'), false); assert.equal(ROOM.test(ROOM_CODE + '\n'), false); assert.equal(isId(CONFIG.participant.id + '\n'), false);
  assert.throws(() => validateConnection({ token: 'offline\n', room: ROOM_CODE })); assert.throws(() => validateConnection({ token: 'offline', room: ROOM_CODE + '\n' }));
  assert.throws(() => requestFor(simple(), { participant: { ...CONFIG.participant, id: CONFIG.participant.id + '\n' } }, 'd1', id(1)));
  const s = await setup(t);
  for (const extra of [{ token: 'offline\n' }, { room: ROOM_CODE + '\n' }, { command: 'recover', attachId: id(1) + '\n' }]) { await assert.rejects(s.run(extra)); assert.equal(s.api.calls.length, 0); }
  await assert.rejects(writeOnce(s.directory, 'recovered.bin\n', Buffer.from('a'))); await assert.rejects(atomicText(s.directory, 'file.json\n', '{}'));
});

test('saved parsed text and monotonic observations remain bound to exact original evidence', async t => {
  const s = await setup(t); await s.run(); s.clock.value = '2026-01-01T09:00:02.000Z'; await s.run();
  s.clock.value = '2026-01-01T09:00:01.000Z'; await assert.rejects(s.run(), /regressed/);
  s.clock.value = '2026-01-01T09:00:03.000Z'; s.api.answer(id(1), cards.shards[0].hex); await s.run();
  const state = await s.saved(); state.attempts[0].observation.answer.hex = 'ff'.repeat(16); await atomicJson(s.directory, 'state.json', state);
  const before = s.api.calls.length; await assert.rejects(s.run()); assert.equal(s.api.calls.length, before);
});

test('prepare publishes matching private manifest and cards; identical reruns succeed and changed payload is preserved for deliberate new preparation', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'parity-prepare-')); t.after(() => rm(directory, { recursive: true, force: true }));
  const payload = Buffer.from([0, 255, 1, 2, 3]); await writeFile(join(directory, 'payload.bin'), payload); await writeFile(join(directory, 'packet-meta.json'), JSON.stringify(metadata));
  const output = await prepareProject(directory), manifest = await readJson(output.manifestPath), cards = await readJson(output.cardsPath);
  assert.equal(output.committed, true); assert.equal(cards.sourceHash, hash(manifest)); assert.equal(recoverPacket(manifest, Object.fromEntries(cards.shards.map(s => [s.id, s.hex]))).packet.payloadHex, payload.toString('hex'));
  assert.equal((await stat(output.cardsPath)).mode & 0o777, 0o600); assert.equal((await stat(output.directory)).mode & 0o777, 0o700);
  assert.deepEqual(await prepareProject(directory), output); const before = await readFile(output.cardsPath);
  await writeFile(join(directory, 'payload.bin'), Buffer.from('different')); await assert.rejects(prepareProject(directory), /different|conflict/); assert.deepEqual(await readFile(output.cardsPath), before);
});

test('interrupted preparation and export resume identical files; no incomplete bundle gets a completion marker', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'parity-prep-failure-')); t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(join(directory, 'payload.bin'), Buffer.from('tiny')); await writeFile(join(directory, 'packet-meta.json'), JSON.stringify(metadata));
  let calls = 0; const interrupted = async (...args) => { if (++calls === 2) throw new Error('Disk failure'); await writeOnce(...args); };
  await assert.rejects(prepareProject(directory, { write: interrupted }), /Disk failure/);
  assert.equal(await readBytes(join(directory, '.parity-prep/complete.json')), null); assert.equal((await prepareProject(directory)).committed, true);
  const s = await setup(t); await decide(s, Object.values(cardTexts())); calls = 0;
  await assert.rejects(exportPacket(s.directory, simple(), { write: interrupted }), /Disk failure/);
  assert.equal(await readBytes(join(s.directory, 'complete.json')), null); assert.equal((await exportPacket(s.directory, simple())).committed, true);
  await writeFile(join(s.directory, 'recovered.bin'), 'tampered'); await assert.rejects(exportPacket(s.directory, simple()), /conflicts/);
});

test('binary input is bounded and refuses symlinks; bundle conflicts and locks prevent replacement', async t => {
  const s = await setup(t); await writeFile(join(s.directory, 'oversized'), Buffer.alloc(97)); await assert.rejects(readBytes(join(s.directory, 'oversized'), 96));
  await symlink(join(s.directory, 'oversized'), join(s.directory, 'link')); await assert.rejects(readBytes(join(s.directory, 'link'), 96));
  await decide(s, Object.values(cardTexts())); const output = await exportPacket(s.directory, simple());
  const unlock = await acquireLock(s.directory); await assert.rejects(exportPacket(s.directory, simple())); await unlock();
  await rm(output.path); await assert.rejects(exportPacket(s.directory, simple()), /conflicts/); assert.equal(await readBytes(output.path), null);
});
