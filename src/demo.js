import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readJson, readBytes } from './storage.js';
import { step, exportPacket } from './engine.js';
import { digest } from './model.js';
import { CONFIG, ROOM_CODE, mockApi } from './mock.js';

const input = await readJson(new URL('../examples/packet.json', import.meta.url));
const cards = await readJson(new URL('../examples/cards.json', import.meta.url));
const hex = cards.shards.map(s => s.hex);
for (const [scenario, choices, expected] of [
  ['one missing data shard recovered', [hex[0], 'Missing', hex[2]], 'complete'],
  ['all data supplied; parity unavailable', [hex[0], hex[1], 'Missing'], 'complete'],
  ['two missing data shards cannot be recovered', ['Missing', 'Missing', hex[2]], 'unrecoverable'],
]) {
  const directory = await mkdtemp(join(tmpdir(), 'pingroom-parity-demo-'));
  try {
    const now = () => '2026-01-01T09:00:00.000Z', api = mockApi(now);
    const options = { client: api.client, directory, input, config: CONFIG, token: 'offline-fixture-token', room: ROOM_CODE, now };
    let result;
    for (const text of choices) {
      const pending = await step(options);
      api.answer(pending.answers.find(a => a.status === 'pending').questionId, text); result = await step(options);
    }
    assert.equal(result.status, expected); assert.equal(api.calls.filter(c => c.method === 'POST').length, 3);
    if (expected === 'complete') { const output = await exportPacket(directory, input); assert.equal(digest(await readBytes(output.path)), input.payloadSha256); }
    else { assert.equal(result.packet, null); await assert.rejects(exportPacket(directory, input)); }
    const calls = api.calls.length; await step(options); assert.equal(api.calls.length, calls);
    console.log(JSON.stringify({ scenario, status: result.status, recoveredShardId: result.packet?.recoveredShardId ?? null,
      parityStatus: result.packet?.parityStatus ?? null, payloadSha256: result.packet?.payloadSha256 ?? null, diagnostics: result.diagnostics, bytesExecuted: false }, null, 2));
  } finally { await rm(directory, { recursive: true, force: true }); }
}
console.log('Offline demo used the pinned SDK with local mock transport. No phone delivery or byte execution. This small non-secret packet exercise is not encryption or secret sharing.');
