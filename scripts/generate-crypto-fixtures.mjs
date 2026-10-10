// Synthetic fixtures only: no account/session state is read.
import { stripTypeScriptTypes, createRequire } from 'node:module'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { createPrivateKey, createPublicKey } from 'node:crypto'
const upstream = resolve(process.argv[2] ?? '../Baileys')
const require = createRequire(resolve('.cache/fixture-tools/package.json'))
const curve = require('libsignal/src/curve.js')
const temp = await mkdtemp(join(tmpdir(), 'baileys-crypto-'))
try {
  const source = stripTypeScriptTypes(await readFile(join(upstream, 'src/Utils/crypto.ts'), 'utf8'))
    .replace("import * as curve from 'libsignal/src/curve'", `import curve from ${JSON.stringify(pathToFileURL(require.resolve('libsignal/src/curve.js')).href)}`)
    .replace("import { KEY_BUNDLE_TYPE } from '../Defaults'", 'const KEY_BUNDLE_TYPE = Buffer.from([5])')
    .replace("'whatsapp-rust-bridge'", JSON.stringify(pathToFileURL(resolve('.cache/fixture-tools/node_modules/whatsapp-rust-bridge/dist/index.js')).href))
  await writeFile(join(temp, 'crypto.mjs'), source)
  const crypto = await import(pathToFileURL(join(temp, 'crypto.mjs')))
  const hex = value => Buffer.from(value).toString('hex')
  const cases = []
  const key = Buffer.from(Array.from({length:32}, (_,i)=>i))
  const iv = Buffer.from(Array.from({length:16}, (_,i)=>31-i))
  const aad = Buffer.from('WhatsApp synthetic additional data')
  for (const length of [0,1,15,16,17,32,255,256,1024]) {
    const data = Buffer.from(Array.from({length}, (_,i)=> i%251))
    cases.push({ data:hex(data), key:hex(key), iv:hex(iv), aad:hex(aad),
      gcm:hex(crypto.aesEncryptGCM(data,key,iv.subarray(0,12),aad)),
      ctr:hex(crypto.aesEncryptCTR(data,key,iv)), cbc:hex(crypto.aesEncrypWithIV(data,key,iv)),
      sha256:hex(crypto.sha256(data)), md5:hex(crypto.md5(data)),
      hmac256:hex(crypto.hmacSign(data,key)), hmac512:hex(crypto.hmacSign(data,key,'sha512')),
      hkdf:hex(crypto.hkdf(data,112,{salt:iv,info:'WhatsApp Image Keys'})) })
  }
  const signatures = []
  const pair = byte => {
    const privateKey = Buffer.alloc(32,byte)
    privateKey[0] &= 248; privateKey[31] = (privateKey[31]&127)|64
    const der = createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b656e04220420','hex'),privateKey]),format:'der',type:'pkcs8'})
    const publicKey = createPublicKey(der).export({format:'der',type:'spki'}).subarray(-32)
    return {private:privateKey, public:publicKey}
  }
  const peer = pair(99)
  for (const byte of [1,2,3,4,5,17,42,255]) {
    const keyPair = pair(byte)
    for (const message of [Buffer.alloc(0),Buffer.from('signed prekey'),Buffer.from('05'+'ab'.repeat(32),'hex')]) {
      const signature = crypto.Curve.sign(keyPair.private,message)
      if (!curve.verifySignature(Buffer.concat([Buffer.from([5]),keyPair.public]),message,Buffer.from(signature))) throw Error('invalid fixture signature')
      signatures.push({private:hex(keyPair.private),public:hex(keyPair.public),message:hex(message),signature:hex(signature),peer:hex(peer.public),shared:hex(crypto.Curve.sharedKey(keyPair.private,peer.public))})
    }
  }
  const pairing = []
  for (const code of ['12345678','ABCD-EFGH','é1234567']) pairing.push({code,salt:hex(iv),key:hex(await crypto.derivePairingCodeKey(code,iv))})
  await writeFile('tests/fixtures/crypto.json',JSON.stringify({cases,signatures,pairing},null,2)+'\n')
  console.log(`Generated ${cases.length} cipher/hash, ${signatures.length} curve, and ${pairing.length} pairing fixtures`)
} finally {await rm(temp,{recursive:true,force:true})}
