// Execute the original MIT incoming receipt handler with event/resend/I/O stubs.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-incoming-receipts-'))
try{
 for(const name of ['constants','jid-utils']){let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}');await writeFile(join(temp,`${name}.mjs`),stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'"))}
 const p=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),p)
 const recv=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),generic=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8'),binary=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8')
 const status=generic.slice(generic.indexOf('const STATUS_MAP'),generic.indexOf('const CODE_MAP'))
 const number=generic.slice(generic.indexOf('export const toNumber'),generic.indexOf('/** unix timestamp'))
 const children=binary.slice(binary.indexOf('const indexCache'),binary.indexOf('export const getAllBinaryNodeChildren'))
 const handle=recv.slice(recv.indexOf('const handleReceipt ='),recv.indexOf('const handleNotification ='))
 await writeFile(join(temp,'handler.mjs'),`import {proto} from './proto.mjs';import {areJidsSameUser,isJidGroup,isJidStatusBroadcast,jidNormalizedUser} from './jid-utils.mjs';`+stripTypeScriptTypes(status+number+children)+stripTypeScriptTypes(`export function make(){const events=[],resends=[],acks=[],counts=new Map();const authState={creds:{me:{id:'100:7@s.whatsapp.net',lid:'900:7@lid'}}};const ev={emit(name,data){events.push({name,data})}},receiptMutex={mutex:fn=>fn()},logger={debug(){},error(){},info(){}};const willSendMessageAgain=async(id,p)=>(counts.get(id+':'+p)||0)<3;const updateSendMessageAgainCount=async(id,p)=>counts.set(id+':'+p,(counts.get(id+':'+p)||0)+1);const sendMessagesAgain=async(key,ids,retry)=>resends.push({key,ids,count:retry?.attrs.count??null});const sendMessageAck=async node=>acks.push(node.attrs.id);`+handle+`return {handleReceipt,events,resends,acks}}`))
 const {make}=await import(pathToFileURL(join(temp,'handler.mjs')))
 const cases=[]
 for(const from of ['200:1@s.whatsapp.net','100:2@s.whatsapp.net','800:1@lid','900:2@lid','group@g.us','status@broadcast'])for(const type of [undefined,'sender','read','read-self','played','inactive','retry']){
  const attrs={id:'A',from,t:'1700000000',...(type?{type}:{}),...((from.includes('g.us')||from==='status@broadcast')?{participant:'800:1@lid'}:{})}
  const node={tag:'receipt',attrs,content:[{tag:'list',attrs:{},content:[{tag:'item',attrs:{id:'B'}},{tag:'item',attrs:{id:'B'}}]},...(type==='retry'?[{tag:'retry',attrs:{count:'2'}}]:[])]}
  const h=make();await h.handleReceipt(node);cases.push({node,events:h.events,resends:h.resends,acks:h.acks})
 }
 for(const attrs of [{id:'A',from:'100:2@s.whatsapp.net',recipient:'200@s.whatsapp.net',type:'read',t:'bad'},{id:'A',from:'group@g.us',type:'read'},{id:'A',from:'group@g.us',participant:'800:1@lid',type:'played',t:'-5.25'},{id:'A',from:'status@broadcast',participant:'900@lid',type:'sender',t:'Infinity'},{id:'A',from:'200@lid',t:''},{id:'A',from:'200@lid',type:'unknown',t:'0x10'}]){
  const node={tag:'receipt',attrs};const h=make();await h.handleReceipt(node);cases.push({node,events:h.events,resends:h.resends,acks:h.acks})
 }
 await writeFile('tests/fixtures/incoming_receipts.json',JSON.stringify({me:'100:7@s.whatsapp.net',lid:'900:7@lid',cases},null,2)+'\n');console.log(`Generated ${cases.length} incoming receipt cases from upstream`)
}finally{await rm(temp,{recursive:true,force:true})}
