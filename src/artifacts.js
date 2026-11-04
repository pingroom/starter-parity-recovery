import { join } from 'node:path';
import { readBytes, writeOnce } from './storage.js';
import { digest } from './model.js';

export const jsonBytes = value => Buffer.from(`${JSON.stringify(value, null, 2)}\n`);

/** Publish immutable files, then a hash-bound completion marker. Caller holds the directory lock. */
export async function writeBundle(directory, files, { write = writeOnce } = {}) {
  const names = Object.keys(files).sort();
  if (!names.length || names.length > 3 || names.some(name => name === 'complete.json' || !/^[a-z][a-z0-9.-]{0,63}(?![\s\S])/.test(name) || !Buffer.isBuffer(files[name]))) throw new Error('Invalid artifact bundle.');
  const marker = jsonBytes({ version: 1, files: names.map(name => ({ name, bytes: files[name].length, sha256: digest(files[name]) })) });
  const previousMarker = await readBytes(join(directory, 'complete.json'));
  if (previousMarker !== null && !previousMarker.equals(marker)) throw new Error('Existing completed bundle belongs to different content. Archive it first.');
  for (const name of names) {
    const existing = await readBytes(join(directory, name));
    if (existing !== null && !existing.equals(files[name]) || previousMarker !== null && existing === null) throw new Error('Existing bundle content conflicts with its expected bytes. Restore or archive it.');
  }
  for (const name of names) await write(directory, name, files[name]);
  await write(directory, 'complete.json', marker);
  for (const [name, bytes] of [...names.map(name => [name, files[name]]), ['complete.json', marker]]) {
    if (!(await readBytes(join(directory, name)))?.equals(bytes)) throw new Error('Artifact verification failed after publication.');
  }
  return { committed: true, directory, marker: join(directory, 'complete.json') };
}
