// Synthetic payload and pairing fixtures from the original upstream helpers.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {createHmac,createPrivateKey,createPublicKey,hkdfSync,createCipheriv,pbkdf2Sync} from 'node:crypto'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json'))
const curve=require('libsignal/src/curve.js'),temp=await mkdtemp(join(tmpdir(),'baileys-connection-'))
const hex=b=>Buffer.from(b).toString('hex')
try{
  let source=await readFile(join(upstream,'WAProto/index.js'),'utf8');source=source.replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),source)
  for(const name of ['constants','jid-utils','encode','decode','generic-utils']){
    source=await readFile(join(upstream,'src/WABinary',`${name}.ts`),'utf8')
    source=source.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains={'+body.replaceAll('=',':')+'}')
    source=stripTypeScriptTypes(source).replace(/import \{\s*\} from '\.\/types';?/, '').replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace("'../../WAProto/index.js'","'./proto.mjs'").replace("import { Boom } from '@hapi/boom'",'class Boom extends Error {}')
    await writeFile(join(temp,`${name}.mjs`),source)
  }
  await writeFile(join(temp,'binary.mjs'),"export * from './jid-utils.mjs';export * from './generic-utils.mjs';")
  await writeFile(join(temp,'defaults.mjs'),"export const KEY_BUNDLE_TYPE=Buffer.from([5]);export const WA_ADV_ACCOUNT_SIG_PREFIX=Buffer.from([6,0]);export const WA_ADV_DEVICE_SIG_PREFIX=Buffer.from([6,1]);export const WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX=Buffer.from([6,5]);")
  await writeFile(join(temp,'crypto.mjs'),`import curve from ${JSON.stringify(pathToFileURL(require.resolve('libsignal/src/curve.js')).href)};import {createHmac} from 'node:crypto';export const Curve={sign:(k,m)=>curve.calculateSignature(k,m),verify:(k,m,s)=>curve.verifySignature(Buffer.concat([Buffer.from([5]),k]),m,Buffer.from(s))};export const hmacSign=(b,k)=>createHmac('sha256',k).update(b).digest();`)
  source=await readFile(join(upstream,'src/Utils/signal.ts'),'utf8');source=source.slice(source.indexOf('export const createSignalIdentity'),source.indexOf('export const getPreKeys'));await writeFile(join(temp,'signal.mjs'),stripTypeScriptTypes("const generateSignalPubKey=k=>Buffer.concat([Buffer.from([5]),k]);\n"+source))
  const generics=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8')
  source=generics.slice(generics.indexOf('export const encodeBigEndian'),generics.indexOf('export const toNumber'))+generics.slice(generics.indexOf('const CROCKFORD_CHARACTERS'),generics.indexOf('export function encodeNewsletterMessage'))
  await writeFile(join(temp,'generics.mjs'),stripTypeScriptTypes(source))
  source=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/validate-connection.ts'),'utf8')).replace("import { Boom } from '@hapi/boom'",'class Boom extends Error {}').replace("'../../WAProto/index.js'","'./proto.mjs'").replace("'../Defaults'","'./defaults.mjs'").replace("'../WABinary'","'./binary.mjs'").replace("'./crypto'","'./crypto.mjs'").replace("'./generics'","'./generics.mjs'").replace("'./signal'","'./signal.mjs'")
  await writeFile(join(temp,'validate.mjs'),source)
  source=await readFile(join(upstream,'src/Utils/companion-reg-client-utils.ts'),'utf8');source=source.replace(/export enum CompanionWebClientType \{([^}]+)\}/,(_,body)=>'export const CompanionWebClientType={'+body.replaceAll('=',':')+'}');await writeFile(join(temp,'companion.mjs'),stripTypeScriptTypes(source))
  const {proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),helpers=await import(pathToFileURL(join(temp,'validate.mjs'))),companion=await import(pathToFileURL(join(temp,'companion.mjs'))),{bytesToCrockford}=await import(pathToFileURL(join(temp,'generics.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
  const authFixture=JSON.parse(await readFile('tests/fixtures/auth.json','utf8'))
  const revive=(_,v)=>v?.type==='Buffer'?Buffer.from(v.data,'base64'):v
  const credentials=JSON.parse(JSON.stringify(authFixture.empty),revive)
  const configurations=[]
  for(const browser of [['Mac OS','Chrome','14.4.1'],['Mac OS','Desktop','14.4.1'],['Windows','Desktop','10'],['Ubuntu','Firefox','22'],['Pixel','Android',''],['Other','Unknown','']])for(const syncFullHistory of [true,false]){
    const config={version:[2,3000,1043857760],browser,syncFullHistory,countryCode:'US',pushName:'Synthetic'}
    configurations.push({config,login:hex(proto.ClientPayload.encode(helpers.generateLoginNode('123456789:7@s.whatsapp.net',config)).finish()),registration:hex(proto.ClientPayload.encode(helpers.generateRegistrationNode(credentials,config)).finish()),qr:companion.buildPairingQRData('ref',credentials.noiseKey.public.toString('base64'),credentials.signedIdentityKey.public.toString('base64'),credentials.advSecretKey,browser)})
  }
  const pair=[]
  const privateKey=Buffer.alloc(32,42);privateKey[0]&=248;privateKey[31]=(privateKey[31]&127)|64
  const der=createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b656e04220420','hex'),privateKey]),format:'der',type:'pkcs8'}),publicKey=createPublicKey(der).export({format:'der',type:'spki'}).subarray(-32)
  for(const hosted of [false,true]){
    const details=proto.ADVDeviceIdentity.encode({rawId:1,timestamp:123,keyIndex:7,deviceType:hosted?1:0}).finish()
    const signature=curve.calculateSignature(privateKey,Buffer.concat([Buffer.from(hosted?[6,5]:[6,0]),details,credentials.signedIdentityKey.public]))
    const account={details,accountSignatureKey:publicKey,accountSignature:signature}
    const signed=proto.ADVSignedDeviceIdentity.encode(account).finish()
    const mac=createHmac('sha256',Buffer.from(credentials.advSecretKey,'base64')).update(Buffer.concat([Buffer.from(hosted?[6,5]:[]),signed])).digest()
    const node={tag:'iq',attrs:{id:'pair'},content:[{tag:'pair-success',attrs:{},content:[{tag:'device-identity',attrs:{},content:proto.ADVSignedDeviceIdentityHMAC.encode({details:signed,hmac:mac,accountType:hosted?1:0}).finish()},{tag:'device',attrs:{jid:'123456789:7@s.whatsapp.net',lid:'987654:7@lid'}},{tag:'platform',attrs:{name:'synthetic'}},{tag:'biz',attrs:{name:'Test'}}]}]}
    const result=helpers.configureSuccessfulPairing(node,credentials)
    pair.push({hosted,stanza:hex(encodeBinaryNode(node)),reply:hex(encodeBinaryNode(result.reply)),account:hex(proto.ADVSignedDeviceIdentity.encode(result.creds.account).finish())})
  }
  const socketSource=await readFile(join(upstream,'src/Socket/socket.ts'),'utf8')
  const requestSource=socketSource.slice(socketSource.indexOf('const requestPairingCode ='),socketSource.indexOf('const sendWAMBuffer ='))
  await writeFile(join(temp,'request.mjs'),stripTypeScriptTypes('export async function build({authState,browser,randomBytes,bytesToCrockford,derivePairingCodeKey,aesEncryptCTR,jidEncode,getCompanionPlatformId}){ let node; const sendNode=async n=>{node=n}; const ev={emit(){}}; const S_WHATSAPP_NET="@s.whatsapp.net"; const generateMessageTag=()=>"code"; '+requestSource+'const code=await requestPairingCode("123456789","ABCDEFGH");return {code,node};}'))
  const {build}=await import(pathToFileURL(join(temp,'request.mjs')))
  const fakeRandom=n=>Buffer.alloc(n,n===16?2:n===12?5:3)
  const aesCTR=(p,k,iv)=>{const c=createCipheriv('aes-256-ctr',k,iv);return Buffer.concat([c.update(p),c.final()])}
  const aesGCM=(p,k,iv,aad)=>{const c=createCipheriv('aes-256-gcm',k,iv);c.setAAD(aad);return Buffer.concat([c.update(p),c.final(),c.getAuthTag()])}
  const hkdf=(p,n,o={})=>Buffer.from(hkdfSync('sha256',p,o.salt??Buffer.alloc(0),o.info??'',n))
  const derivePairingCodeKey=async (code,salt)=>pbkdf2Sync(code,salt,131072,32,'sha256')
  const phoneCreds=JSON.parse(JSON.stringify(authFixture.empty),revive)
  const request=await build({authState:{creds:phoneCreds},browser:['Mac OS','Chrome','14.4.1'],randomBytes:fakeRandom,bytesToCrockford,derivePairingCodeKey,aesEncryptCTR:aesCTR,jidEncode:(u,s)=>`${u}@${s}`,getCompanionPlatformId:companion.getCompanionPlatformId})
  const phoneSalt=Buffer.alloc(32,3),phoneIv=Buffer.alloc(16,2),phoneKey=await derivePairingCodeKey('ABCDEFGH',phoneSalt)
  const notification={tag:'notification',attrs:{},content:[{tag:'link_code_companion_reg',attrs:{},content:[{tag:'link_code_pairing_ref',attrs:{},content:Buffer.from('ref')},{tag:'primary_identity_pub',attrs:{},content:publicKey},{tag:'link_code_pairing_wrapped_primary_ephemeral_pub',attrs:{},content:Buffer.concat([phoneSalt,phoneIv,aesCTR(publicKey,phoneKey,phoneIv)])}]}]}
  const recvSource=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8')
  let finishSource=recvSource.slice(recvSource.indexOf("case 'link_code_companion_reg':")+"case 'link_code_companion_reg':".length,recvSource.indexOf("case 'privacy_token':"))
  finishSource=finishSource.replace(/break\s*$/, '')
  await writeFile(join(temp,'finish.mjs'),stripTypeScriptTypes('export async function finish({authState,node,Curve,randomBytes,hkdf,aesEncryptGCM,derivePairingCodeKey,aesDecryptCTR,getBinaryNodeChild,getBinaryNodeChildBuffer}){let reply;const query=async n=>{reply=n};const sock={generateMessageTag:()=>"finish"};const S_WHATSAPP_NET="@s.whatsapp.net";const ev={emit(){}};const toRequiredBuffer=b=>Buffer.from(b);async function decipherLinkPublicKey(b){return aesDecryptCTR(b.subarray(48,80),await derivePairingCodeKey(authState.creds.pairingCode,b.subarray(0,32)),b.subarray(32,48))};'+finishSource+'return {reply,advSecretKey:authState.creds.advSecretKey};}'))
  const {finish}=await import(pathToFileURL(join(temp,'finish.mjs'))),binary=await import(pathToFileURL(join(temp,'generic-utils.mjs')))
  const finishResult=await finish({authState:{creds:phoneCreds},node:notification,Curve:{sharedKey:(k,p)=>curve.calculateAgreement(Buffer.concat([Buffer.from([5]),p]),k)},randomBytes:fakeRandom,hkdf,aesEncryptGCM:aesGCM,derivePairingCodeKey,aesDecryptCTR:aesCTR,getBinaryNodeChild:binary.getBinaryNodeChild,getBinaryNodeChildBuffer:binary.getBinaryNodeChildBuffer})
  const phone={request:hex(encodeBinaryNode(request.node)),notification:hex(encodeBinaryNode(notification)),finish:hex(encodeBinaryNode(finishResult.reply)),advSecretKey:finishResult.advSecretKey}
  const crockford=[Buffer.alloc(5),Buffer.alloc(5,255),Buffer.from([1,2,3,4,5])].map(b=>({input:hex(b),output:bytesToCrockford(b)}))
  await writeFile('tests/fixtures/connection.json',JSON.stringify({configurations,pair,crockford,phone},null,2)+'\n')
  console.log('Generated 12 payload/browser cases, standard/hosted pairing, and upstream Crockford fixtures')
}finally{await rm(temp,{recursive:true,force:true})}
