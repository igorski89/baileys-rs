// Execute original MIT retry sending/key helpers with synthetic auth and I/O stubs.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-retry-request-'))
const hex=b=>Buffer.from(b).toString('hex'),reviver=(_k,v)=>v?.type==='Buffer'?Buffer.from(v.data,'base64'):v
try {
 let source=await readFile(join(upstream,'WAProto/index.js'),'utf8');await writeFile(join(temp,'proto.mjs'),source.replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
 for(const name of ['constants','jid-utils','encode']){let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}');await writeFile(join(temp,`${name}.mjs`),stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,''))}
 let decode=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/decode-wa-message.ts'),'utf8')).replaceAll("'../../WAProto/index.js'","'./proto.mjs'").replaceAll("'../WABinary'","'./jid-utils.mjs'").replaceAll("'./generics'","'./unused-generics.mjs'").replace("import { Boom } from '@hapi/boom'","class Boom extends Error {}")
 await writeFile(join(temp,'decode.mjs'),decode);await writeFile(join(temp,'unused-generics.mjs'),'export function unpadRandomMax16(){throw Error("not invoked by envelope oracle")}')
 const recv=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),signal=await readFile(join(upstream,'src/Utils/signal.ts'),'utf8'),generic=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8'),conn=await readFile(join(upstream,'src/Utils/validate-connection.ts'),'utf8')
 const retry=recv.slice(recv.indexOf('const sendRetryRequest ='),recv.indexOf('// Mirrors WAWeb/Handle/PreKeyLow.js'))
 const prekeys=signal.slice(signal.indexOf('export const getPreKeys ='),signal.indexOf('const isValidUInt'))+signal.slice(signal.indexOf('export const getNextPreKeys ='),signal.indexOf('export const getNextPreKeysNode ='))
 const big=generic.slice(generic.indexOf('export const encodeBigEndian'),generic.indexOf('export const toNumber'))
 const identity=conn.slice(conn.indexOf('export const encodeSignedDeviceIdentity ='))
 await writeFile(join(temp,'request.mjs'),`import {proto} from './proto.mjs';import {decodeMessageNode} from './decode.mjs';import {jidDecode} from './jid-utils.mjs';const KEY_BUNDLE_TYPE=Buffer.from([5]);const Curve={generateKeyPair(){throw Error('fixture should reuse stored prekeys')}};`+stripTypeScriptTypes(prekeys+big+identity)+stripTypeScriptTypes(`export function make(initial,options){const creds=structuredClone(initial),sent=[],updates=[],deleted=[],phone=[],counts=new Map(),recentCounts=new Map(),prekeys={};for(let id=1;id<=16;id++)prekeys[id]=initial.noiseKey;const logger={debug(){},warn(){},info(){}};const authState={creds,keys:{async set(change){if(change.session)deleted.push(...Object.keys(change.session));if(change['pre-key'])Object.assign(prekeys,change['pre-key'])},async get(_category,ids){return Object.fromEntries(ids.filter(id=>prekeys[id]).map(id=>[id,prekeys[id]]))},async transaction(work){await work()}}};const ev={emit(_name,update){updates.push(update);Object.assign(creds,update)}};const msgRetryCache={async set(k,v){counts.set(k,v)},async get(k){return counts.get(k)},async del(k){counts.delete(k)}};const messageRetryManager=options.manager?{hasExceededMaxRetries(id){return (recentCounts.get(id)||0)>=options.max},markRetryFailed(id){recentCounts.delete(id)},incrementRetryCount(id){const count=(recentCounts.get(id)||0)+1;recentCounts.set(id,count);return count},shouldRecreateSession(_jid,exists){return {recreate:!exists||options.recreate,reason:'fixture'}},schedulePhoneRequest(id){phone.push({id,scheduled:true})}}:null;const maxMsgRetryCount=options.max,enableAutoSessionRecreation=options.auto;const signalRepository={jidToSignalProtocolAddress(jid){const d=jidDecode(jid);return d.user+(d.domainType?'_'+d.domainType:'')+'.'+(d.device||0)},async validateSession(){return {exists:options.hasSession}}};const requestPlaceholderResend=async key=>{phone.push({id:key.id,scheduled:false});return 'PHONE'};const sendNode=async node=>sent.push(node);`+retry+`return {sendRetryRequest,sent,updates,deleted,phone,creds}}`))
 const {proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),{make}=await import(pathToFileURL(join(temp,'request.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
 const auth=JSON.parse(await readFile('tests/fixtures/auth.json','utf8'),reviver).empty,connection=JSON.parse(await readFile('tests/fixtures/connection.json','utf8'))
 const credentials={...auth,account:proto.ADVSignedDeviceIdentity.decode(Buffer.from(connection.pair[0].account,'hex')),me:{id:'100:7@s.whatsapp.net',lid:'987654:7@lid'},nextPreKeyId:17,firstUnuploadedPreKeyId:1}
 const cases=[]
 for(const options of [{manager:true,max:3,auto:false,hasSession:true},{manager:true,max:3,auto:true,hasSession:false},{manager:false,max:3,auto:true,hasSession:true}]){
  const h=make(credentials,options),node={tag:'message',attrs:{id:'RETRY',from:'200:1@s.whatsapp.net',t:'1700000000',participant:'200@s.whatsapp.net'}}
  const steps=[];for(let attempt=0;attempt<5;attempt++){const before=h.sent.length;await h.sendRetryRequest(node,attempt===4);steps.push({wire:h.sent.length>before?hex(encodeBinaryNode(h.sent.at(-1))):null,first:h.creds.firstUnuploadedPreKeyId,next:h.creds.nextPreKeyId})}
  cases.push({options,node,steps,phone:h.phone,deleted:h.deleted,updates:h.updates})
 }
 const serialization=JSON.parse(JSON.stringify(credentials,(_k,v)=>v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:v))
 await writeFile('tests/fixtures/retry_requests.json',JSON.stringify({credentials:serialization,cases},null,2)+'\n')
 console.log(`Generated ${cases.length} original retry-request sequences`)
}finally{await rm(temp,{recursive:true,force:true})}
