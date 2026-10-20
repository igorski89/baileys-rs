// Run the TypeScript connector's pure helpers on sample inputs.
// node scripts/generate-helper-fixtures.mjs ../baileys-sqs
import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { stripTypeScriptTypes } from 'node:module'
import { createHash } from 'node:crypto'

const source = await readFile(join(process.argv[2] ?? '../baileys-sqs', 'index.ts'), 'utf8')
const slice = (start, end) => {
  const from = source.indexOf(start)
  const to = source.indexOf(end, from)
  if (from < 0 || to < 0) throw new Error(`missing ${start}`)
  return source.slice(from, to)
}
const code = [
  slice('const getFifoGroupId', '// Deterministic JSON.stringify'),
  slice('const canonicalStringify', '// Only alphanumerics'),
  slice('const sanitizeForDedupId', '// Human-readable prefix'),
  slice('const getDedupPrefix', '// Content-based dedup id'),
  slice('const computeDedupId', 'const sendToQueue'),
  slice('const normalizeJid', '// ===== URL fetch'),
  slice('const extensionFromMimetype', 'const uploadMediaToS3'),
].join('\n')
const helpers = new Function('createHash', stripTypeScriptTypes(code) +
  '\nreturn { getFifoGroupId, canonicalStringify, sanitizeForDedupId, getDedupPrefix, computeDedupId, normalizeJid, extensionFromMimetype }')(createHash)

const bodies = [
  { id: 'e1', type: 'baileys_event', event: 'messages.upsert', meta: { type: 'notify' }, payload: { messages: [{ key: { remoteJid: '1@s.whatsapp.net', id: '3EB0A', fromMe: false }, messageTimestamp: '1700000000' }], type: 'notify' } },
  { id: 'e2', type: 'baileys_event', event: 'messages.upsert', meta: {}, payload: { messages: [{ key: { id: 'NOJID' } }], type: 'append' } },
  { id: 'e3', type: 'baileys_event', event: 'messages.update', meta: {}, payload: [{ key: { remoteJid: '120363@g.us', id: 'ID 😀/x' }, update: { status: 4 } }] },
  { id: 'e4', type: 'baileys_event', event: 'messages.update', meta: {}, payload: [{ key: { remoteJid: '1@s.whatsapp.net', id: 'S' }, update: { starred: true } }] },
  { id: 'e5', type: 'baileys_event', event: 'presence.update', meta: {}, payload: { id: '1@s.whatsapp.net', presences: { '1@s.whatsapp.net': { lastKnownPresence: 'composing' } } } },
  { id: 'e6', type: 'baileys_event', event: 'chats.update', meta: {}, payload: [{ id: '1@s.whatsapp.net', unreadCount: 1.5, b: [1, 'two', null, { z: 1, a: 2 }] }] },
  { type: 'command_ack', payload: { correlation_id: 'job #7', command_type: 'send_text', to: '1@s.whatsapp.net', ok: true, message_id: '3EB0' } },
  { type: 'command_ack', payload: { command_type: 'send_presence', to: '1@s.whatsapp.net', ok: true, message_id: null } },
  { type: 'command_ack', payload: { command_type: 'send_text', to: null, ok: false, error: 'x' } },
  { type: 'qr', payload: { qr: 'ref,key' } },
  { type: 'connection', payload: { status: 'connected' } },
  { type: 'baileys_event', event: 'contacts.upsert', payload: [{ id: 'é@s.whatsapp.net' }] },
  { id: 'e7', type: 'baileys_event', event: 'messages.upsert', meta: {}, payload: { messages: [{ key: { remoteJid: 'x'.repeat(80) + '@s.whatsapp.net', id: 'LONG' } }] } },
  {},
]
const cases = bodies.map(body => ({
  body,
  groupId: helpers.getFifoGroupId(body),
  prefix: helpers.getDedupPrefix(body),
  canonical: helpers.canonicalStringify(body),
  dedupId: helpers.computeDedupId(body),
}))
const jids = ['+1 234 567 890', '1234567890', '123@g.us', '\t+49 151 123']
  .map(to => ({ to, jid: helpers.normalizeJid(to) }))
const mimetypes = ['image/jpeg', 'audio/ogg; codecs=opus', 'application/vnd.ms-excel', 'text/', 'plain', undefined, '']
  .map(mimetype => ({ mimetype: mimetype ?? null, extension: helpers.extensionFromMimetype(mimetype) }))
await writeFile('tests/fixtures/helpers.json', JSON.stringify({ cases, jids, mimetypes }, null, 2) + '\n')
console.log(`wrote ${cases.length} bodies, ${jids.length} jids, ${mimetypes.length} mimetypes`)
