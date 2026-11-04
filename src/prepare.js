import { join } from 'node:path';
import { acquireLock, readBytes, readJson } from './storage.js';
import { packPacket, hash } from './model.js';
import { writeBundle, jsonBytes } from './artifacts.js';
import { root } from './config.js';

export async function prepareProject(projectRoot = root, options = {}) {
  const metadata = await readJson(join(projectRoot, 'packet-meta.json'), 4096), payload = await readBytes(join(projectRoot, 'payload.bin'), 96);
  if (metadata === null || payload === null) throw new Error('Prepare requires packet-meta.json and a 1–96-byte payload.bin in this project.');
  const { manifest, cards } = packPacket(metadata, payload), directory = join(projectRoot, '.parity-prep');
  const release = await acquireLock(directory);
  try {
    const publication = await writeBundle(directory, { 'packet.json': jsonBytes(manifest), 'cards.json': jsonBytes(cards) }, options);
    return { status: 'complete', ...publication, manifestPath: join(directory, 'packet.json'), cardsPath: join(directory, 'cards.json'), sourceHash: hash(manifest) };
  } finally { await release(); }
}
