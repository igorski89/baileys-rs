// Run upstream Noise against a synthetic server and certificate authority.
import { stripTypeScriptTypes, createRequire } from 'node:module'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { createPrivateKey, createPublicKey } from 'node:crypto'
const upstream = resolve(process.argv[2] ?? '../Baileys')
const require = createRequire(resolve('.cache/fixture-tools/package.json'))
const protobuf = require('protobufjs')
const root = await protobuf.load(join(upstream,'WAProto/WAProto.proto'))
const temp = await mkdtemp(join(tmpdir(),'baileys-noise-'))
const hex = b => Buffer.from(b).toString('hex')
const pair = byte => {
  const privateKey=Buffer.alloc(32,byte); privateKey[0]&=248; privateKey[31]=(privateKey[31]&127)|64
  const der=createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b656e04220420','hex'),privateKey]),format:'der',type:'pkcs8'})
  return {private:privateKey,public:createPublicKey(der).export({format:'der',type:'spki'}).subarray(-32)}
}
const rootKey=pair(20),intermediateKey=pair(21),clientEphemeral=pair(10),clientStatic=pair(11),serverEphemeral=pair(12),serverStatic=pair(13)
try {
  for (const name of ['constants','jid-utils','encode','decode']) {
    let source=await readFile(join(upstream,'src/WABinary',`${name}.ts`),'utf8')
    source=source.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains = {'+body.replaceAll('=',':')+'}')
    source=stripTypeScriptTypes(source).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'")
    await writeFile(join(temp,`${name}.mjs`),source)
  }
  let source=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/crypto.ts'),'utf8'))
    .replace("import * as curve from 'libsignal/src/curve'",`import curve from ${JSON.stringify(pathToFileURL(require.resolve('libsignal/src/curve.js')).href)}`)
    .replace("import { KEY_BUNDLE_TYPE } from '../Defaults'",'const KEY_BUNDLE_TYPE = Buffer.from([5])')
    .replace("'whatsapp-rust-bridge'",JSON.stringify(pathToFileURL(resolve('.cache/fixture-tools/node_modules/whatsapp-rust-bridge/dist/index.js')).href))
  await writeFile(join(temp,'crypto.mjs'),source)
  await writeFile(join(temp,'defaults.mjs'),`export const NOISE_MODE = 'Noise_XX_25519_AESGCM_SHA256\\0\\0\\0\\0'; export const WA_CERT_DETAILS={SERIAL:0,PUBLIC_KEY:Buffer.from('${hex(rootKey.public)}','hex')}`)
  // The generated proto wrapper is replaced only at the serialization adapter:
  // protobufjs loads the same upstream schema and emits identical wire messages.
  await writeFile(join(temp,'proto.mjs'),`import protobuf from ${JSON.stringify(pathToFileURL(require.resolve('protobufjs')).href)}; const root=await protobuf.load(${JSON.stringify(join(upstream,'WAProto/WAProto.proto'))}); const chain=root.lookupType('proto.CertChain'); chain.NoiseCertificate={Details:root.lookupType('proto.CertChain.NoiseCertificate.Details')}; export const proto={CertChain:chain};`)
  source=await readFile(join(upstream,'src/Utils/noise-handler.ts'),'utf8')
  source=source.replace(/constructor\(\s*private readonly encKey: Uint8Array,\s*private readonly decKey: Uint8Array\s*\) \{\}/, 'constructor(encKey: Uint8Array, decKey: Uint8Array) { this.encKey = encKey; this.decKey = decKey }')
  source=stripTypeScriptTypes(source)
    .replace("import { Boom } from '@hapi/boom'",'class Boom extends Error {}')
    .replace("'../../WAProto/index.js'","'./proto.mjs'")
    .replace("'../Defaults'","'./defaults.mjs'")
    .replace("'../WABinary'","'./decode.mjs'")
    .replace("'./crypto'","'./crypto.mjs'")
  await writeFile(join(temp,'noise.mjs'),source)
  const crypto=await import(pathToFileURL(join(temp,'crypto.mjs')))
  const {makeNoiseHandler}=await import(pathToFileURL(join(temp,'noise.mjs')))
  const {encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
  const header=Buffer.from([87,65,6,3])
  const encode=(type,object)=>Buffer.from(root.lookupType(`proto.${type}`).encode(object).finish())
  const details=object=>encode('CertChain.NoiseCertificate.Details',object)
  const logger={child(){return this},trace(){},level:'silent'}
  class Server {
    hash=Buffer.from('Noise_XX_25519_AESGCM_SHA256\0\0\0\0'); salt=this.hash; key=this.hash; counter=0
    constructor(){this.auth(header);this.auth(clientEphemeral.public)}
    auth(b){this.hash=crypto.sha256(Buffer.concat([this.hash,b]))}
    mix(b){const k=crypto.hkdf(b,64,{salt:this.salt,info:''});this.salt=k.subarray(0,32);this.key=k.subarray(32);this.counter=0}
    iv(){const iv=Buffer.alloc(12);iv.writeUInt32BE(this.counter++,8);return iv}
    encrypt(b){const out=crypto.aesEncryptGCM(b,this.key,this.iv(),this.hash);this.auth(out);return out}
    decrypt(b){const out=crypto.aesDecryptGCM(b,this.key,this.iv(),this.hash);this.auth(b);return out}
    hello(kind){
      this.auth(serverEphemeral.public);this.mix(crypto.Curve.sharedKey(serverEphemeral.private,clientEphemeral.public))
      const staticCipher=this.encrypt(serverStatic.public)
      this.mix(crypto.Curve.sharedKey(serverStatic.private,clientEphemeral.public))
      const intermediateDetails=details({serial:1,issuerSerial:kind==='issuer'?9:0,key:intermediateKey.public})
      const leafDetails=details({serial:2,issuerSerial:1,key:serverStatic.public})
      const intermediateSignature=crypto.Curve.sign(rootKey.private,intermediateDetails)
      const leafSignature=crypto.Curve.sign(intermediateKey.private,leafDetails)
      if(kind==='leaf')leafSignature[0]^=1
      if(kind==='intermediate')intermediateSignature[0]^=1
      const cert=encode('CertChain',{intermediate:{details:intermediateDetails,signature:intermediateSignature},leaf:kind==='missing'?undefined:{details:leafDetails,signature:leafSignature}})
      return {serverHello:{ephemeral:serverEphemeral.public,static:staticCipher,payload:this.encrypt(cert)}}
    }
  }
  const fixtures={root:hex(rootKey.public),ephemeralPrivate:hex(clientEphemeral.private),staticPrivate:hex(clientStatic.private),invalid:[]}
  for(const kind of ['leaf','intermediate','issuer','missing'])fixtures.invalid.push({kind,hello:hex(encode('HandshakeMessage',new Server().hello(kind)))})
  const server=new Server(),hello=server.hello('valid')
  fixtures.hello=hex(encode('HandshakeMessage',hello))
  const handler=makeNoiseHandler({keyPair:clientEphemeral,NOISE_HEADER:header,logger})
  const encryptedStatic=handler.processHandshake(hello,clientStatic)
  fixtures.encryptedStatic=hex(encryptedStatic)
  if(hex(server.decrypt(encryptedStatic))!==hex(clientStatic.public))throw Error('bad client static')
  server.mix(crypto.Curve.sharedKey(serverEphemeral.private,clientStatic.public))
  const payload=Buffer.from('synthetic client payload')
  fixtures.clientPayload=hex(payload);fixtures.encryptedPayload=hex(handler.encrypt(payload))
  if(hex(server.decrypt(Buffer.from(fixtures.encryptedPayload,'hex')))!==hex(payload))throw Error('bad client payload')
  const transportKeys=crypto.hkdf(Buffer.alloc(0),64,{salt:server.salt,info:''})
  await handler.finishInit()
  const node={tag:'iq',attrs:{id:'ABC123',type:'result'},content:[{tag:'ok',attrs:{}}]}
  const nodeBytes=encodeBinaryNode(node)
  fixtures.nodeBytes=hex(nodeBytes)
  fixtures.clientFrames=[hex(handler.encodeFrame(nodeBytes)),hex(handler.encodeFrame(nodeBytes))]
  fixtures.serverFrames=[]
  for(let counter=0;counter<2;counter++){
    const iv=Buffer.alloc(12);iv.writeUInt32BE(counter,8)
    const ciphertext=crypto.aesEncryptGCM(nodeBytes,transportKeys.subarray(32),iv,Buffer.alloc(0))
    const frame=Buffer.alloc(3+ciphertext.length);frame.writeUIntBE(ciphertext.length,0,3);frame.set(ciphertext,3)
    fixtures.serverFrames.push(hex(frame))
  }
  const routed=makeNoiseHandler({keyPair:clientEphemeral,NOISE_HEADER:header,logger,routingInfo:Buffer.from('routing')})
  fixtures.routedFrame=hex(routed.encodeFrame(Buffer.from('hello')))
  await writeFile('tests/fixtures/noise.json',JSON.stringify(fixtures,null,2)+'\n')
  console.log('Generated full upstream Noise handshake, transport and certificate rejection fixtures')
} finally {await rm(temp,{recursive:true,force:true})}
