import { createHash } from 'node:crypto';

export const TTL = 900;
export const CLOCK_SKEW_MS = 60000;
export const TOKEN = /^[^\s\x00-\x1f\x7f]{1,8192}(?![\s\S])/;
export const ROOM = /^[A-Z0-9]{12}$(?![\s\S])/;
export const isId = (value) => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/.test(value);
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const keys = (value, expected) => {
  if (!record(value)) return false;
  const actual = Object.keys(value).sort(), wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
};
const word = (value) => typeof value === 'string' && /^[a-z][a-z0-9_-]{0,39}$(?![\s\S])/.test(value);
const line = (value, max) => typeof value === 'string' && value.length > 0 && value.length <= max && value.isWellFormed()
  && !/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(value) && value.trim() === value;
const canonical = (value) => Array.isArray(value) ? value.map(canonical) : record(value) ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])])) : value;
export const hash = (value) => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

export function iso(raw) {
  if (typeof raw !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$(?![\s\S])/.test(raw)) throw new Error('Expected a UTC timestamp.');
  const time = Date.parse(raw);
  if (!Number.isFinite(time) || new Date(time).toISOString().slice(0, 19) !== raw.slice(0, 19)) throw new Error('Invalid UTC timestamp.');
  return new Date(time).toISOString();
}


export const SHARD_BYTES = 16;
export const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const sha = value => typeof value === 'string' && /^[0-9a-f]{64}(?![\s\S])/.test(value);
export function validateMetadata(value) {
  if (!keys(value, ['packetId', 'title', 'context']) || !word(value.packetId) || !line(value.title, 40) || !line(value.context, 80)) throw new Error('Packet metadata requires a safe packetId, title up to 40 and context up to 80 UTF-16 units.');
  return structuredClone(value);
}
export function validateInput(input) {
  if (!keys(input, ['packetId', 'title', 'context', 'dataLength', 'shardBytes', 'shards', 'payloadSha256'])) throw new Error('Invalid packet manifest keys.');
  validateMetadata({ packetId: input.packetId, title: input.title, context: input.context });
  if (!Number.isSafeInteger(input.dataLength) || input.dataLength < 1 || input.dataLength > 96 || input.shardBytes !== SHARD_BYTES || !sha(input.payloadSha256)
    || !Array.isArray(input.shards) || input.shards.length !== Math.ceil(input.dataLength / SHARD_BYTES) + 1
    || input.shards.some((shard, index) => !keys(shard, ['id', 'sha256']) || shard.id !== (index === input.shards.length - 1 ? 'p' : `d${index + 1}`) || !sha(shard.sha256))) throw new Error('Use 1–96 payload bytes, fixed 16-byte shards, ordered d1…dN then p, and lowercase SHA-256 digests.');
  return structuredClone(input);
}
export function validateConfig(config) {
  if (!keys(config, ['participant']) || !keys(config.participant, ['id', 'label']) || !isId(config.participant.id) || !line(config.participant.label, 60)) throw new Error('Config requires an exact packet operator UUID and label.');
  return structuredClone(config);
}
export const itemsFor = input => validateInput(input).shards;
export function parseShard(text) {
  if (typeof text !== 'string' || text.length > 32 || !text.isWellFormed()) throw new Error('Expected a bounded normalized text response.');
  if (text === 'Missing') return { kind: 'missing', hex: null };
  if (!/^[a-fA-F0-9]{32}(?![\s\S])/.test(text)) return { kind: 'invalid', hex: null };
  return { kind: 'hex', hex: text.toLowerCase() };
}
function xor(shards) {
  const result = Buffer.alloc(SHARD_BYTES);
  for (const shard of shards) for (let i = 0; i < SHARD_BYTES; i++) result[i] ^= shard[i];
  return result;
}
export function packPacket(rawMetadata, payload) {
  const metadata = validateMetadata(rawMetadata);
  if (!(payload instanceof Uint8Array) || payload.length < 1 || payload.length > 96) throw new Error('payload.bin must contain 1–96 bytes.');
  payload = Buffer.from(payload);
  const chunks = Array.from({ length: Math.ceil(payload.length / SHARD_BYTES) }, (_, i) => {
    const chunk = Buffer.alloc(SHARD_BYTES); payload.copy(chunk, 0, i * SHARD_BYTES, Math.min(payload.length, (i + 1) * SHARD_BYTES)); return chunk;
  });
  const values = [...chunks, xor(chunks)];
  const manifest = validateInput({ ...metadata, dataLength: payload.length, shardBytes: SHARD_BYTES,
    shards: values.map((bytes, i) => ({ id: i === chunks.length ? 'p' : `d${i + 1}`, sha256: digest(bytes) })), payloadSha256: digest(payload) });
  return { manifest, cards: { packetId: metadata.packetId, sourceHash: hash(manifest), shards: values.map((bytes, i) => ({ id: manifest.shards[i].id, hex: bytes.toString('hex') })) } };
}
export function requestFor(rawInput, config, itemId, attemptId) {
  const input = validateInput(rawInput), shard = input.shards.find(item => item.id === itemId); validateConfig(config);
  if (!shard || !isId(attemptId)) throw new Error('Invalid shard or attempt.');
  const prompt = `${input.title}\nContext: ${input.context}\nPacket: ${input.packetId}\nShard: ${shard.id} (${shard.id === 'p' ? 'XOR parity' : 'data'})\nTranscribe its 32 hex characters, or exactly Missing if unavailable. Do not guess. Non-secret practice packet only.`;
  if (prompt.length > 500) throw new Error('Shard prompt exceeds 500 characters.');
  const key = `parity-${attemptId}`;
  return { prompt, context: `To ${config.participant.id}`, text_input: { placeholder: '32 hex characters or Missing', max_length: 32 },
    responder_scope: 'direct', target_user_id: config.participant.id, ttl: TTL, correlation_id: key,
    data: { input_hash: hash(input), item_id: itemId, attempt_id: attemptId }, idempotencyKey: key };
}

/** Recover at most one declared data erasure; corrupt observations are never erasures. */
export function recoverPacket(rawInput, texts) {
  const input = validateInput(rawInput);
  if (!keys(texts, input.shards.map(s => s.id)) || Object.values(texts).some(text => text !== null && (typeof text !== 'string' || text.length > 32 || !text.isWellFormed()))) throw new Error('Supply an exact bounded text or null for each shard.');
  const diagnostics = [], outcomes = input.shards.map(shard => {
    const text = texts[shard.id];
    if (text === null) return { id: shard.id, text, kind: 'unresolved', hex: null, checksum: 'not_checked' };
    const parsed = parseShard(text);
    const checksum = parsed.kind === 'hex' ? digest(Buffer.from(parsed.hex, 'hex')) === shard.sha256 ? 'matched' : 'mismatch' : 'not_checked';
    if (parsed.kind === 'invalid') diagnostics.push({ code: 'invalid_hex', shardId: shard.id });
    if (checksum === 'mismatch') diagnostics.push({ code: 'shard_checksum_mismatch', shardId: shard.id });
    return { id: shard.id, text, ...parsed, checksum };
  });
  const unresolvedShardIds = outcomes.filter(o => o.kind === 'unresolved').map(o => o.id), missingShardIds = outcomes.filter(o => o.kind === 'missing').map(o => o.id);
  const base = { outcomes, unresolvedShardIds, missingShardIds, diagnostics, packet: null };
  if (diagnostics.length) return { ...base, status: 'invalid' };
  if (unresolvedShardIds.length) return { ...base, status: 'incomplete' };
  const data = outcomes.slice(0, -1), parity = outcomes.at(-1), missing = data.filter(o => o.kind === 'missing');
  if (missing.length > 1 || missing.length && parity.kind === 'missing') {
    diagnostics.push({ code: 'insufficient_shards', missingDataCount: missing.length, parityAvailable: parity.kind === 'hex' });
    return { ...base, status: 'unrecoverable' };
  }
  const chunks = data.map(o => o.kind === 'hex' ? Buffer.from(o.hex, 'hex') : null);
  let recoveredShardId = null;
  if (missing.length === 1) {
    recoveredShardId = missing[0].id; const index = data.findIndex(o => o.id === recoveredShardId);
    chunks[index] = xor([Buffer.from(parity.hex, 'hex'), ...chunks.filter(Boolean)]);
    if (digest(chunks[index]) !== input.shards[index].sha256) diagnostics.push({ code: 'reconstructed_checksum_mismatch', shardId: recoveredShardId });
  }
  const joined = Buffer.concat(chunks), payload = joined.subarray(0, input.dataLength);
  if (joined.subarray(input.dataLength).some(byte => byte !== 0)) diagnostics.push({ code: 'nonzero_padding', shardId: data.at(-1).id });
  if (parity.kind === 'hex' && !xor(chunks).equals(Buffer.from(parity.hex, 'hex'))) diagnostics.push({ code: 'inconsistent_parity', shardId: 'p' });
  if (digest(payload) !== input.payloadSha256) diagnostics.push({ code: 'payload_checksum_mismatch' });
  if (diagnostics.length) return { ...base, status: 'invalid' };
  return { ...base, status: 'complete', packet: { packetId: input.packetId, dataLength: input.dataLength, payloadHex: payload.toString('hex'), payloadSha256: digest(payload),
    recoveredShardId, parityStatus: parity.kind === 'hex' ? 'verified' : 'unavailable', paddingVerified: true, suppliedChecksumsVerified: true, bytesExecuted: false } };
}

export function validateState(state, rawInput) {
  const input = validateInput(rawInput), tasks = itemsFor(input);
  if (!keys(state, ['version', 'inputHash', 'config', 'room', 'credentialHash', 'startedAt', 'attempts']) || state.version !== 1
    || state.inputHash !== hash(input) || typeof state.room !== 'string' || !ROOM.test(state.room)
    || typeof state.credentialHash !== 'string' || state.credentialHash.length !== 64 || !/^[0-9a-f]{64}$/.test(state.credentialHash)
    || !Array.isArray(state.attempts) || state.attempts.length > tasks.length || iso(state.startedAt) !== state.startedAt) {
    throw new Error('Invalid state or changed packet manifest. Restore the original inputs.');
  }
  validateConfig(state.config);
  const ids = new Set(), questionIds = new Set();
  let previous = state.startedAt, askerId;
  for (const [index, attempt] of state.attempts.entries()) {
    if (!keys(attempt, ['id', 'itemId', 'createdAt', 'request', 'questionId', 'status', 'observation'])
      || !isId(attempt.id) || ids.has(attempt.id) || attempt.itemId !== tasks[index].id
      || iso(attempt.createdAt) !== attempt.createdAt || attempt.createdAt < previous
      || hash(attempt.request) !== hash(requestFor(input, state.config, attempt.itemId, attempt.id))
      || !['creating', 'pending', 'answered', 'expired', 'cancelled', 'invalid_answer'].includes(attempt.status)) {
      throw new Error('Invalid saved shard request.');
    }
    ids.add(attempt.id);
    previous = attempt.createdAt;
    if (attempt.status === 'creating') {
      if (attempt.questionId !== null || attempt.observation !== null) throw new Error('Invalid uncertain creation state.');
    } else {
      if (!isId(attempt.questionId) || questionIds.has(attempt.questionId)) throw new Error('Invalid or repeated Question ID.');
      questionIds.add(attempt.questionId);
      validateObservation(attempt.observation, attempt.status, attempt.request);
      if (Date.parse(attempt.observation.createdAt) < Date.parse(attempt.createdAt) - CLOCK_SKEW_MS
        || attempt.observation.checkedAt < attempt.createdAt) throw new Error('Observation predates creation intent.');
      previous = attempt.observation.checkedAt;
      if (askerId !== undefined && attempt.observation.askerId !== askerId) throw new Error('Questions belong to different asking registrations.');
      askerId = attempt.observation.askerId;
    }
    if (['creating', 'pending'].includes(attempt.status) && index !== state.attempts.length - 1) {
      throw new Error('Only the last task may have an active Question.');
    }
  }
  return state;
}

export function report(state, rawInput) {
  const input = validateInput(rawInput); validateState(state, input);
  const answers = itemsFor(input).map(shard => {
    const attempt = state.attempts.find(row => row.itemId === shard.id), answer = attempt?.observation?.answer;
    return { shardId: shard.id, status: attempt?.status === 'creating' ? 'create_unknown' : attempt?.status ?? 'unasked',
      text: answer?.text ?? null, kind: answer?.kind ?? null, hex: answer?.hex ?? null,
      questionId: attempt?.questionId ?? null, attemptId: attempt?.id ?? null,
      responderId: answer?.responderId ?? null, answeredAt: answer?.answeredAt ?? null,
      askerId: attempt?.observation?.askerId ?? null, observedAt: attempt?.observation?.checkedAt ?? null };
  });
  const calculation = recoverPacket(input, Object.fromEntries(answers.map(a => [a.shardId, a.text])));
  const active = answers.find(row => ['create_unknown', 'pending'].includes(row.status));
  const status = active?.status ?? (answers.some(row => row.status === 'unasked') ? 'ready' : calculation.status);
  return { packetId: input.packetId, source: input, sourceHash: hash(input), participant: state.config.participant, room: state.room,
    answers, ...calculation, status, bytesExecuted: false };
}

function validateObservation(observation, status, request) {
  if (!keys(observation, ['createdAt', 'expiresAt', 'checkedAt', 'askerId', 'resolvedAt', 'answer']) || !isId(observation.askerId)) throw new Error('Invalid saved observation.');
  for (const key of ['createdAt', 'expiresAt', 'checkedAt']) if (iso(observation[key]) !== observation[key]) throw new Error('Invalid saved timestamp.');
  const duration = Date.parse(observation.expiresAt) - Date.parse(observation.createdAt);
  if (duration < TTL * 1000 - 1000 || duration > TTL * 1000 + 1000) throw new Error('Question lifetime differs from the saved request.');
  const latest = Date.parse(observation.checkedAt) + CLOCK_SKEW_MS;
  if (Date.parse(observation.createdAt) > latest) throw new Error('Question creation is implausibly in the future. Check the local clock.');
  if (status === 'pending') {
    if (observation.resolvedAt !== null) throw new Error('Pending Question has a resolution timestamp.');
  } else if (iso(observation.resolvedAt) !== observation.resolvedAt || observation.resolvedAt < observation.createdAt
    || Date.parse(observation.resolvedAt) > latest
    || (status === 'expired' ? observation.resolvedAt < observation.expiresAt : observation.resolvedAt > observation.expiresAt)) throw new Error('Invalid Question resolution time.');
  if (status === 'answered') {
    const answer = observation.answer;
    if (!keys(answer, ['text', 'kind', 'hex', 'responderId', 'answeredAt']) || answer.responderId !== request.target_user_id
      || hash(parseShard(answer.text)) !== hash({ kind: answer.kind, hex: answer.hex })
      || iso(answer.answeredAt) !== answer.answeredAt || answer.answeredAt !== observation.resolvedAt
      || answer.answeredAt < observation.createdAt || answer.answeredAt > observation.expiresAt) throw new Error('Invalid saved shard evidence.');
  } else if (observation.answer !== null) throw new Error('Unanswered tasks must not contain shard evidence.');
}

export function observeQuestion(q, request, questionId, room, checkedAt) {
  if (!isId(questionId) || q?.id !== questionId || q.kind !== 'question' || q.is_handoff !== false
    || q.asker?.type !== 'agent' || !isId(q.asker?.id) || q.prompt !== request.prompt || q.context !== request.context
    || q.responder_scope !== 'direct' || q.target_user_id !== request.target_user_id || q.resolution_policy !== 'first'
    || q.results !== null || hash(q.text_input) !== hash(request.text_input) || q.correlation_id !== request.correlation_id || q.reply_to !== null || hash(q.data) !== hash(request.data)
    || (q.room_code ?? q.room?.code) !== room || q.room_code !== undefined && q.room_code !== room
    || q.room !== undefined && q.room?.code !== room || !['pending', 'answered', 'expired', 'cancelled'].includes(q.state)
    || !Array.isArray(q.options) || q.options.length !== 0) throw new Error('Question does not match the saved task, room, or packet operator.');
  const observation = { createdAt: iso(q.created_at), expiresAt: iso(q.expires_at), checkedAt: iso(checkedAt), askerId: q.asker.id,
    resolvedAt: q.resolved_at === null ? null : iso(q.resolved_at), answer: null };
  let status = q.state === 'answered' ? 'invalid_answer' : q.state;
  if (q.state !== 'answered' && q.answer !== null) throw new Error('Unresolved Question includes an unexpected answer.');
  if (q.state === 'answered') {
    const answer = q.answer;
    try {
      if (!answer || answer.value !== null || answer.label !== null || answer.responder?.id !== request.target_user_id || typeof answer.text !== 'string' || answer.text.length > 32) throw new Error('Invalid text answer.');
      observation.answer = { text: answer.text, ...parseShard(answer.text), responderId: answer.responder.id, answeredAt: iso(answer.answered_at) };
      validateObservation(observation, 'answered', request); status = 'answered';
    } catch { observation.answer = null; }
  }
  validateObservation(observation, status, request);
  return { status, observation };
}

export function assertParticipantRoom(rooms, room, participantId) {
  if (!Array.isArray(rooms) || rooms.length > 1000) throw new Error('Invalid room inventory.');
  const matches = rooms.filter((item) => item?.invite_code === room), selected = matches[0];
  if (matches.length !== 1 || selected.type !== 'standard' || selected.is_public !== false || !Array.isArray(selected.members)
    || selected.members.length > 1000 || !selected.members.some((member) => member?.user_id === participantId)) throw new Error('Use a granted standard private room containing the named participant.');
}
