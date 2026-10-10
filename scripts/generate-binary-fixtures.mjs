// Execute the local upstream codec without installing its unrelated runtime dependencies.
import { stripTypeScriptTypes } from 'node:module'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { deflateSync } from 'node:zlib'

const upstream = resolve(process.argv[2] ?? '../Baileys')
const temp = await mkdtemp(join(tmpdir(), 'baileys-codec-'))
try {
  for (const file of ['constants', 'jid-utils', 'encode', 'decode']) {
    let source = await readFile(join(upstream, 'src/WABinary', `${file}.ts`), 'utf8')
    // Node 26 strips types but no longer transforms TypeScript enums.
    source = source.replace(/export enum WAJIDDomains \{([^}]+)\}/, (_, body) =>
      'export const WAJIDDomains = {' + body.replaceAll('=', ':') + '}')
    source = stripTypeScriptTypes(source, { mode: 'strip' })
      .replaceAll("'./constants'", "'./constants.mjs'")
      .replaceAll("'./jid-utils'", "'./jid-utils.mjs'")
    await writeFile(join(temp, `${file}.mjs`), source)
  }
  const { encodeBinaryNode } = await import(pathToFileURL(join(temp, 'encode.mjs')))
  const { decodeBinaryNode } = await import(pathToFileURL(join(temp, 'decode.mjs')))
  const { SINGLE_BYTE_TOKENS, DOUBLE_BYTE_TOKENS } = await import(pathToFileURL(join(temp, 'constants.mjs')))
  const rustNode = node => ({
    tag: node.tag, attrs: node.attrs,
    content: node.content === undefined ? null :
      Array.isArray(node.content) ? { Children: node.content.map(rustNode) } :
      typeof node.content === 'string' ? { Text: node.content } : { Bytes: [...node.content] }
  })
  const cases = [
    ['empty', { tag: 'iq', attrs: {} }],
    ['message', { tag: 'message', attrs: { id: 'ABCDEF123', to: '123456789:2@s.whatsapp.net', type: 'text' }, content: [{ tag: 'body', attrs: {}, content: 'hello Rust' }] }],
    ['empty-user', { tag: 'iq', attrs: { to: '@s.whatsapp.net' } }],
    ['empty-content', { tag: 'body', attrs: {}, content: '' }],
    ['empty-children', { tag: 'list', attrs: {}, content: [] }],
    ['unicode', { tag: 'body', attrs: { name: '東京' }, content: 'Olá 世界' }],
    ['packed', { tag: 'iq', attrs: { id: '1-234.5', hex: 'DEADBEEF', lower: 'deadbeef', empty: '' } }],
    ...['s.whatsapp.net', 'lid', 'hosted', 'hosted.lid'].flatMap(server => [0, 1, 255].map(device => [`${server}-${device}`, { tag: 'iq', attrs: { to: `12345:${device}@${server}` } }])),
    ...[0, 1, 255, 256, 1048575, 1048576].map(length => [`bytes-${length}`, { tag: 'enc', attrs: {}, content: Buffer.alloc(length, 42) }]),
    ['list16', { tag: 'iq', attrs: {}, content: Array.from({ length: 256 }, () => ({ tag: 'item', attrs: {} })) }],
    ...SINGLE_BYTE_TOKENS.filter(Boolean).map((token, index) => [`single-${index}`, { tag: 'iq', attrs: { value: token } }]),
    ...DOUBLE_BYTE_TOKENS.flatMap((dict, d) => dict.map((token, i) => [`double-${d}-${i}`, { tag: 'iq', attrs: { value: token } }])),
  ]
  const fixtures = []
  for (const [name, input] of cases) {
    const wire = encodeBinaryNode(input)
    // Large uniform payloads use a compact record expanded by the Rust test harness.
    if (Buffer.isBuffer(input.content) && input.content.length > 256) {
      fixtures.push({ name, repeated: { length: input.content.length, byte: 42 }, prefix: [...wire.subarray(0, wire.length - input.content.length)] })
    } else fixtures.push({ name, input: rustNode(input), wire: [...wire], decoded: rustNode(await decodeBinaryNode(wire)) })
  }
  const input = cases[1][1]
  const compressed = Buffer.concat([Buffer.from([2]), deflateSync(encodeBinaryNode(input).subarray(1))])
  fixtures.push({ name: 'compressed', wire: [...compressed], decoded: rustNode(await decodeBinaryNode(compressed)) })
  await writeFile('tests/fixtures/binary.json', JSON.stringify(fixtures, null, 2) + '\n')
  await writeFile('proto/tokens.json', JSON.stringify({ single: SINGLE_BYTE_TOKENS, double: DOUBLE_BYTE_TOKENS }) + '\n')
  console.log(`Generated ${fixtures.length} upstream parity fixtures`)
} finally { await rm(temp, { recursive: true, force: true }) }
