# PingRoom parity recovery

Recover a small, non-secret packet from human-transcribed hex shards and one XOR parity shard. A named operator supplies surviving shards through PingRoom; code exports a local packet after every integrity check passes. This bounded transcription exercise provides no backup system, encryption or secret sharing.

## Try it offline

Use Node.js 22 or 24 on macOS or Linux. Choose **Use this template**, clone your repository, then run:

```sh
npm ci
npm test
npm run demo
```

The demo uses the pinned SDK with a local mock transport. It covers one missing data shard, complete data with missing parity, and two missing data shards. Tests and demo need no credentials or network after installation. Live phone delivery is not tested.

## Prepare a practice packet

Start with the included harmless paper craft label:

```sh
cp examples/packet.json packet.json
cp config.example.json config.json
cat examples/cards.json
npm run preview
```

`cards.json` contains shard contents; `packet.json` contains metadata and expected digests. The fixture has two data shards, `d1` and `d2`, followed by parity `p`. Transcribe `d1` and `p`, and answer **Missing** for `d2`.

To prepare your own non-secret bytes **before starting a session**:

```sh
cp examples/packet-meta.json packet-meta.json
# Edit packetId, title and context in packet-meta.json.
printf 'PAPER CRAFT | My practice label\n' > payload.bin
npm run prepare-packet
# Only after successful preparation:
cp .parity-prep/packet.json packet.json
cat .parity-prep/cards.json
npm run preview
```

The helper reads fixed project files `payload.bin` and `packet-meta.json`. Payloads may contain arbitrary binary data, 1–96 bytes long. It creates 16-byte data shards, zero-pads the final shard, computes XOR parity, and writes ignored `.parity-prep/packet.json`, `cards.json` and `complete.json`. Cards contain plaintext-equivalent data; private permissions do not make them secret shares.

Preparation writes **`complete.json` last**; other files may exist before that marker. A successful command verifies all files and hashes. Identical reruns resume; changed content is refused. Archive the preparation directory securely before preparing another packet. Copy only a successfully prepared manifest and keep it associated with those exact cards.

## Connect the operator

Install [PingRoom](https://pingroom.io/i), sign in, and choose a **standard private room** containing the operator:

```sh
npm run connect
npm run inspect
```

Open the pairing URL, approve the connection and grant the room. Verify the person in the app, then copy their exact member UUID from `inspect` into `config.json` as `participant.id`. Names alone do not establish identity. Retain the robot's grant and operator's membership.

Pairing saves ignored `.pingroom/credentials.json` and resumes a pending link. For an **existing connection only**, copy `.env.example` to `.env` and set both `PINGROOM_TOKEN` and `PINGROOM_ROOM`. Complete pairs override saved credentials. Either shell variable selects the whole shell pair without borrowing from `.env`; blank or partial pairs fail. Other CLI credential stores are not read.

## Transcribe and export

```sh
npm start          # Send the next shard, or read its active Question once.
# Answer in PingRoom, then repeat:
npm start
npm run status     # Read saved evidence offline.
npm run report     # Save .parity/report.json, including partial evidence.
npm run export     # Export only a fully verified packet.
```

Each Question shows packet identity, context and shard ID without its expected contents. Enter exactly 32 hexadecimal characters, uppercase or lowercase, or **Missing** when the card is unavailable; never substitute guessed bytes. PingRoom may trim or constrain submitted text. Validation checks the returned normalized text, not the original keystrokes. Valid hex is converted to lowercase without changing bytes.

At most seven Questions are created in data-shard order, then parity, each lasting 15 minutes. Every Question must resolve, including parity: unanswered does not mean Missing. Collection continues after a bad transcription without replacing it. Attribution, request identity, asking registration and timestamps are checked before accepting text. Invalid envelopes, expired or cancelled Questions block export.

## Interpret recovery

| Supplied observations | Result after all checks |
| --- | --- |
| All data, with parity | Verify payload and parity consistency. |
| All data, parity Missing | Verify payload; `parityStatus` is `unavailable`. |
| Exactly one data shard Missing, parity supplied | Reconstruct that data shard and verify it. |
| Multiple data shards Missing, or data and parity Missing | `unrecoverable`; no packet export. |
| Malformed text, mismatched digest, padding or parity | `invalid`; retain diagnostics and refuse export. |

The algorithm XORs parity with surviving data to recover one erasure. This follows the same elementary XOR recovery principle described in the [Linux kernel parity documentation](https://www.kernel.org/doc/html/v6.10/driver-api/md/raid5-ppl.html); this starter does not implement RAID.

Every supplied padded-shard SHA-256 must match. Reconstructed bytes must match the missing shard's expected hash. Final padding must be zero, supplied parity must agree, and the unpadded payload hash must match. Corrupt input is never silently treated as Missing. Hashes detect inconsistency relative to the supplied manifest; they do not authenticate its author or protect a replaced manifest.

`answers` retains attributed normalized transcriptions. `outcomes` records parsing and checksum checks; `diagnostics`, `missingShardIds` and `unresolvedShardIds` explain failures. `packet` stays null until verification succeeds. A valid zero-filled payload remains valid bytes.

Export writes fixed `.parity/recovered.bin`, `recovered.json` provenance and, last, `complete.json`. Partial files alone are not a completed export. Conflicting files or missing files under an existing marker are refused. Require successful export for the expected source hash. Before consuming bytes, require `complete.json`, verify each listed file's byte length and SHA-256, then verify the payload hash against the manifest. The starter neither parses nor executes recovered bytes; review them before use.

## Recover and adapt

Creation intent is saved before POST. At `create_unknown`, preserve state:

```sh
npm run find-id
npm run recover -- EXACT_QUESTION_UUID
```

An empty bounded recent-list search does not prove creation failed. Recover the exact original Question without resending; never delete state to bypass uncertainty. For uncertain/expired pairing, resolve or revoke the old robot in Connected Agents before archiving `.pingroom/pairing.json`. Before a new packet, resolve outstanding Questions and securely archive `.parity/`.

Collection exits: `0` verified packet, `1` error, `2` incomplete/invalid/unrecoverable or newly failed Question, `3` ready/pending/interrupted, `4` uncertain creation. Readable status/report commands exit `0`; inspect their JSON status. Refused exports exit `1`. No scheduler is installed.

`src/model.js` owns packing, parsing and recovery; `src/engine.js` owns collection/export. Use validated session reports when provenance matters. Deterministic byte operations require no AI integration.

The manifest fixes `shardBytes: 16`, payload length and SHA-256 digests. Shards must appear as `d1`…`dN`, then `p`; order cannot change. Metadata limits in UTF-16 units: packet ID 40, title 40, context 80, operator label 60. Packet IDs use a lowercase initial letter, then lowercase letters, digits, `_` or `-`. Text must be nonempty, well formed, single line, without control/format characters or outer whitespace. Digests are exactly 64 lowercase hex characters.

Input/config caps are 64/16 KiB; preparation metadata 4 KiB; state/reports 1 MiB; HTTP bodies 512 KiB. Private files use `0600`, directories `0700`. Local inputs, credentials, preparation and recovery outputs are Git-ignored. Paths resolve from this project. Use one machine with persistent storage. Remove a stale `.lock` only after confirming its recorded process stopped; restore corrupted state from backup. Saved evidence can be altered; persistence does not guarantee distributed exactly-once execution. HTTP requests refuse redirects and use five-second deadlines.

Tests cover an independent parity oracle, binary/padding limits, transaction interruption, recovery, pairing and actual CLI subprocesses. Actions runs Node 22/24. [SDK](https://github.com/pingroom/sdk) `0.7.1` is pinned with a lockfile and 14-day npm minimum release age. [MIT license](LICENSE).
