// Deterministic synthetic credentials made by the original initAuthCreds function.
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {createPrivateKey,createPublicKey} from 'node:crypto'
const upstream=resolve(process.argv[2]??'../Baileys')
const require=createRequire(resolve('.cache/fixture-tools/package.json'))
const curve=require('libsignal/src/curve.js')
const protobuf=require('protobufjs')
const root=await protobuf.load(join(upstream,'WAProto/WAProto.proto'))
const temp=await mkdtemp(join(tmpdir(),'baileys-auth-'))
try {
  const auth=await readFile(join(upstream,'src/Utils/auth-utils.ts'),'utf8')
  const generics=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8')
  const buffer=generics.slice(generics.indexOf('export const BufferJSON'),generics.indexOf('export const getKeyAuthor'))
  const init=auth.slice(auth.indexOf('export const initAuthCreds'))
  let index=1
  const pair=()=>{
    const privateKey=Buffer.alloc(32,index++);privateKey[0]&=248;privateKey[31]=(privateKey[31]&127)|64
    const der=createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b656e04220420','hex'),privateKey]),format:'der',type:'pkcs8'})
    return {private:privateKey,public:createPublicKey(der).export({format:'der',type:'spki'}).subarray(-32)}
  }
  await writeFile(join(temp,'init.mjs'),stripTypeScriptTypes('export function create(Curve,signedKeyPair,randomBytes,generateRegistrationId){'+init.replace('export const','const')+'; return initAuthCreds() }\n'+buffer))
  const {create,BufferJSON}=await import(pathToFileURL(join(temp,'init.mjs')))
  const creds=create({generateKeyPair:pair},(identity,keyId)=>{
    const keyPair=pair();return{keyPair,keyId,signature:curve.calculateSignature(identity.private,Buffer.concat([Buffer.from([5]),keyPair.public]))}
  },n=>Buffer.alloc(n,77),()=>42)
  const empty=JSON.parse(JSON.stringify(creds,BufferJSON.replacer))
  creds.me={id:'test:7@s.whatsapp.net',name:'Synthetic test user',imgUrl:null}
  creds.registered=true;creds.routingInfo=Buffer.from('synthetic routing');creds.additionalData={example:true};creds.customFutureField='preserved'
  creds.signalIdentities=[{identifier:{name:'test@s.whatsapp.net',deviceId:0},identifierKey:Buffer.concat([Buffer.from([5]),creds.signedIdentityKey.public])}]
  creds.account={details:Buffer.from('synthetic'),accountSignatureKey:Buffer.alloc(32,9),accountSignature:Buffer.alloc(64,9),deviceSignature:Buffer.alloc(64,10)}
  creds.processedHistoryMessages=[{key:{remoteJid:'test@lid',id:'ABC',fromMe:true},messageTimestamp:protobuf.util.Long.fromString('18446744073709551615',true)}]
  const key=root.lookupType('proto.Message.AppStateSyncKeyData').fromObject({keyData:Buffer.alloc(32,7),fingerprint:{rawId:1,currentIndex:2,deviceIndexes:[3,4]},timestamp:'9223372036854775807'})
  const keys={
    'pre-key':creds.signedPreKey.keyPair,
    'session':{_sessions:{synthetic:{registrationId:42,chainKey:{counter:1,key:{type:'Buffer',data:'AQID'}}}},version:'v1'},
    'sender-key':Buffer.from('{"senderKeyStates":[]}'),
    'sender-key-memory':{'test:7@lid':true},
    'app-state-sync-key':key,
    'app-state-sync-version':{version:7,hash:Buffer.alloc(128,0),indexValueMap:{synthetic:{valueMac:Buffer.alloc(32,8)}}},
    'lid-mapping':'test@lid','device-list':['test:7@s.whatsapp.net'],
    tctoken:{token:Buffer.from('token'),timestamp:'123',senderTimestamp:456},'identity-key':Buffer.alloc(33,5)
  }
  await writeFile('tests/fixtures/auth.json',JSON.stringify({empty,populated:JSON.parse(JSON.stringify(creds,BufferJSON.replacer)),keys:JSON.parse(JSON.stringify(keys,BufferJSON.replacer))},null,2)+'\n')
  console.log('Generated upstream credential defaults, BufferJSON, and all 10 key categories')
}finally{await rm(temp,{recursive:true,force:true})}
