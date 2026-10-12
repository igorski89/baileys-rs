// Execute the original MIT group/picture/account_sync notification branches and handleNotification wrapper.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-notifications-'))
const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):v
try{
 for(const name of ['constants','jid-utils','generic-utils']){
  let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
  s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}")
  s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
  await writeFile(join(temp,`${name}.mjs`),s)
 }
 await writeFile(join(temp,'proto.mjs'),(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
 const groups=await readFile(join(upstream,'src/Socket/groups.ts'),'utf8'),recv=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),decode=await readFile(join(upstream,'src/Utils/decode-wa-message.ts'),'utf8')
 const imports=`import {getAllBinaryNodeChildren,getBinaryNodeChild,getBinaryNodeChildren,getBinaryNodeChildString} from './generic-utils.mjs';import {areJidsSameUser,isJidGroup,isLidUser,isPnUser,jidEncode,jidNormalizedUser} from './jid-utils.mjs';import {proto} from './proto.mjs';const WAMessageStubType=proto.WebMessageInfo.StubType,WAMessageAddressingMode={LID:'lid',PN:'pn'};`
 const metadata=groups.slice(groups.indexOf('export const extractGroupMetadata'))
 const addressing=decode.slice(decode.indexOf('export const extractAddressingContext'),decode.indexOf('/**\n * Decode the received node as a message.'))
 const group=recv.slice(recv.indexOf('const handleGroupNotification ='),recv.indexOf('const handleDevicesNotification ='))
 const processing=recv.slice(recv.indexOf('const processNotification ='),recv.indexOf('\t/**\n\t * In-memory cache of storage JIDs'))
 const handle=recv.slice(recv.indexOf('const handleNotification ='),recv.indexOf('const handleMessage ='))
 if([metadata,addressing,group,processing,handle].some(s=>!s.length))throw Error('upstream slice not found')
 const factory=`export function make(){const events=[],upserts=[],acks=[];const authState={creds:{me:{id:'100:7@s.whatsapp.net',lid:'900:7@lid'},accountSettings:{unarchiveChats:true}}};const ev={emit(name,data){events.push({name,data:JSON.parse(JSON.stringify(data))})}},notificationMutex={mutex:fn=>fn()},logger={debug(){},error(){},info(){},warn(){}};const unsupported=async()=>{};const handleNewsletterNotification=unsupported,handleMexNotification=unsupported,handleEncryptNotification=unsupported,handleDevicesNotification=unsupported,handlePrivacyTokenNotification=unsupported,resyncAppState=unsupported;const upsertMessage=async(m,kind)=>upserts.push({kind,wire:Buffer.from(proto.WebMessageInfo.encode(m).finish()).toString('hex')});const sendMessageAck=async node=>acks.push(node.attrs.id??null);`
 await writeFile(join(temp,'handler.mjs'),imports+stripTypeScriptTypes(metadata+addressing+factory+group+processing+handle+`return {handleNotification,events,upserts,acks}}`))
 const {make}=await import(pathToFileURL(join(temp,'handler.mjs')))
 const copy=n=>({tag:n.tag,attrs:{...n.attrs},...(n.content===undefined?{}:{content:Array.isArray(n.content)?n.content.map(copy):n.content})})
 const node=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),cases=[]
 const wireNode=n=>({tag:n.tag,attrs:n.attrs,...(n.content===undefined?{}:{content:Array.isArray(n.content)?n.content.map(wireNode):Buffer.isBuffer(n.content)?{type:'Buffer',data:n.content.toString('base64')}:n.content})})
 async function run(n){const h=make();let error;try{await h.handleNotification(n)}catch(e){error=e.message}cases.push({node:wireNode(n),events:h.events.map(e=>({name:e.name,data:clean(e.data)})),upserts:h.upserts,acks:h.acks,error:error!==undefined})}
 const roots=[
  {id:'N1',from:'group@g.us',type:'w:gp2',t:'1700000000',participant:'300@lid',participant_pn:'300@s.whatsapp.net',participant_username:'alice'},
  {id:'N2',from:'group@g.us',type:'w:gp2',t:'1700000001'},
  {id:'N3',from:'group@g.us',type:'w:gp2',t:'1700000002',participant:'100:3@s.whatsapp.net',participant_lid:'900@lid',addressing_mode:'pn'},
  {id:'N4',from:'group@g.us',type:'w:gp2',t:'1700000003',participant:'300@s.whatsapp.net',participant_lid:'300@lid'},
  {id:'N5',from:'group@g.us',type:'w:gp2',t:'1700000004',participant:'',participant_pn:'300@s.whatsapp.net',sender_pn:'301@s.whatsapp.net',addressing_mode:'lid'}
 ]
 const p=(attrs={})=>node('participant',attrs)
 const groupNode=(extra={},participants=[p({jid:'300@lid',phone_number:'300@s.whatsapp.net',type:'superadmin'}),p({jid:'400@s.whatsapp.net',lid:'400@lid'})])=>node('group',{id:'group',subject:'Subject ☕',s_t:'1699999999',creation:'1699999000',creator:'300@lid',creator_pn:'300@s.whatsapp.net',...extra},participants)
 const children=[
  node('create',{},[groupNode()]),
  node('create',{},[groupNode({creator:undefined,creator_pn:undefined,subject:undefined,creation:'bad',addressing_mode:'lid'},[])]),
  node('create',{},[groupNode({id:'other@g.us',creator:'300@s.whatsapp.net',creator_pn:undefined},[p({jid:'300@s.whatsapp.net'})])]),
  node('ephemeral',{expiration:'86400'}),node('ephemeral',{}),node('ephemeral',{expiration:'-1'}),node('ephemeral',{expiration:'1.9'}),node('ephemeral',{expiration:'bad'}),node('ephemeral',{expiration:'4294967296'}),node('not_ephemeral',{}),node('not_ephemeral',{expiration:'7'}),
  node('modify',{},[p({jid:'300@s.whatsapp.net'}),p({jid:'301@s.whatsapp.net'})]),node('modify',{}),node('modify',{},[p({})]),
  ...['promote','demote','remove','add','leave'].flatMap(tag=>[
   node(tag,{},[p({jid:'300@lid',phone_number:'300@s.whatsapp.net',type:'admin'})]),
   node(tag,{},[p({jid:'400@s.whatsapp.net',lid:'400@lid',participant_username:'bob',username:'ignored'}),p({jid:'500@lid',phone_number:'500@lid',username:'carol',type:''})]),
   node(tag,{},[p({jid:'300@s.whatsapp.net',lid:'300@s.whatsapp.net'})]),
   node(tag,{},[p({})]),node(tag,{}),
   node(tag,{},[p({jid:'600"\\\n☕@lid',phone_number:'600@s.whatsapp.net',username:'',type:'superadmin'})])
  ]),
  node('subject',{subject:'New subject ☕'}),node('subject',{subject:''}),node('subject',{}),
  node('description',{},[node('body',{},Buffer.from('Description ☕'))]),node('description',{},[node('body',{},'string body')]),node('description',{},[node('body',{},Buffer.alloc(0))]),node('description',{},[node('body',{})]),node('description',{}),node('description',{},[node('delete',{})]),
  node('announcement',{}),node('not_announcement',{}),node('locked',{}),node('unlocked',{}),
  node('invite',{code:'CODE'}),node('invite',{}),
  node('member_add_mode',{},Buffer.from('all_member_add')),node('member_add_mode',{},'admin_add'),node('member_add_mode',{}),node('member_add_mode',{},Buffer.alloc(0)),
  node('membership_approval_mode',{},[node('group_join',{state:'on'})]),node('membership_approval_mode',{},[node('group_join',{})]),node('membership_approval_mode',{}),
  node('created_membership_requests',{request_method:'invite_link'},[p({jid:'700@lid',phone_number:'700@s.whatsapp.net'})]),node('created_membership_requests',{},[p({jid:'',phone_number:''})]),node('created_membership_requests',{request_method:'non_admin_add'}),
  node('revoked_membership_requests',{},[p({jid:'300@lid',phone_number:'300@s.whatsapp.net'})]),node('revoked_membership_requests',{},[p({jid:'700@lid',phone_number:'700@s.whatsapp.net'})]),node('revoked_membership_requests',{}),
  node('unknown',{})
 ]
 for(const root of roots)for(const child of children)await run(node('notification',root,[copy(child)]))
 await run(node('notification',roots[0]));await run(node('notification',roots[0],[]))
 for(const t of [undefined,'','bad','-5.25','Infinity','-Infinity','0x10','1.9','18446744073709551616','9007199254740993',' 12 '])await run(node('notification',{...roots[0],t},[node('subject',{subject:'T'})]))
 for(const attrs of [{id:undefined},{from:undefined},{from:'',participant:'300@lid'},{from:'300@s.whatsapp.net'},{from:'group@g.us',participant:'100@s.whatsapp.net'},{from:'100:9@s.whatsapp.net',participant:undefined},{from:'group@g.us',participant:'900:7@lid'}])await run(node('notification',{...roots[0],...attrs},[node('subject',{subject:'K'})]))
 // Picture changes: contacts.update for every chat, plus a stub message for groups.
 const pictures=[[node('set',{id:'PIC',author:'300@lid',hash:'HASH'})],[node('set',{id:'PIC'})],[node('set',{})],[node('delete',{author:'300@lid',hash:'DEL'})],[node('delete',{})],[node('set',{id:'PIC',author:'300@lid'}),node('delete',{author:'400@lid'})],[node('delete',{author:'400@lid'}),node('set',{author:'300@lid'})],[],[node('other',{})]]
 for(const from of ['group@g.us','200:3@s.whatsapp.net','200@lid','',undefined,'status@broadcast'])for(const participant of [undefined,'500@lid'])for(const content of pictures)await run(node('notification',{id:'P1',from,type:'picture',t:'1700000005',participant},content.map(copy)))
 await run(node('notification',{id:'P2',from:'group@g.us',type:'picture',t:'1700000005'}))
 // Account synchronisation.
 for(const attrs of [{duration:'86400',t:'1700000006'},{duration:'0',t:'0'},{},{duration:'bad',t:'bad'},{duration:'-1',t:'1.5'},{duration:'4294967296',t:'-7'}])await run(node('notification',{id:'A1',from:'100@s.whatsapp.net',type:'account_sync',t:'1700000007'},[node('disappearing_mode',attrs)]))
 for(const items of [[node('item',{jid:'200@s.whatsapp.net',action:'block'}),node('item',{jid:'201@lid',action:'unblock'}),node('item',{action:'block'}),node('item',{jid:'202@lid'})],[],undefined,[node('other',{jid:'x'})]])await run(node('notification',{id:'A2',from:'100@s.whatsapp.net',type:'account_sync',t:'1700000007'},[node('blocklist',{},items)]))
 await run(node('notification',{id:'A3',from:'100@s.whatsapp.net',type:'account_sync'},[node('other',{})]));await run(node('notification',{id:'A4',from:'100@s.whatsapp.net',type:'account_sync'}));await run(node('notification',{id:'A5',from:'100@s.whatsapp.net',type:'account_sync'},[]))
 for(const type of [undefined,'unknown','status'])await run(node('notification',{id:'U1',from:'200@s.whatsapp.net',type},[node('subject',{subject:'ignored'})]))
 // Device-list notifications against a seeded device cache.
 const devicesSource=recv.slice(recv.indexOf('const handleDevicesNotification ='),recv.indexOf('const processNotification ='))
 await writeFile(join(temp,'devices.mjs'),`import {getAllBinaryNodeChildren,getBinaryNodeChildren} from './generic-utils.mjs';import {areJidsSameUser,jidDecode,jidNormalizedUser} from './jid-utils.mjs';`+stripTypeScriptTypes(`export function make(seed,failDelete){const cache=new Map(Object.entries(structuredClone(seed))),deleted=[];const authState={creds:{me:{id:'100:7@s.whatsapp.net',lid:'900:7@lid'}}},logger={debug(){},error(){},info(){},warn(){}},devicesMutex={mutex:fn=>fn()};const userDevicesCache={get:async k=>cache.get(k),set:async(k,v)=>{cache.set(k,v)},del:async k=>{cache.delete(k)}};const signalRepository={deleteSession:async jids=>{if(failDelete)throw Error('delete failure');deleted.push(jids)}};`+devicesSource+`return {handleDevicesNotification,cache,deleted}}`))
 const {make:makeDevices}=await import(pathToFileURL(join(temp,'devices.mjs'))),deviceCases=[]
 const d=jid=>node('device',jid===undefined?{}:{jid}),entry=(user,device,server='s.whatsapp.net')=>({user,server,device})
 const seeds=[{},{'200':[entry('200',0),entry('200',1),entry('200',2)]},{'200':[entry('200',1)],'300':[entry('300',0,'lid'),entry('300',5,'lid')]}]
 const lists=[[d('200:1@s.whatsapp.net')],[d('200:3@s.whatsapp.net'),d('200:1@s.whatsapp.net')],[d('200@s.whatsapp.net')],[d('200:1@s.whatsapp.net'),d('300:5@lid'),d('200:2@s.whatsapp.net'),d('300:9@lid')],[d('200:0@s.whatsapp.net'),d('200:1@s.whatsapp.net'),d('200:2@s.whatsapp.net')],[d(),d('nojid'),d('')],[d(),d('400:4@s.whatsapp.net')],[]]
 for(const seed of seeds)for(const tag of ['add','remove','update','other'])for(const list of lists)for(const failDelete of tag==='remove'?[false,true]:[false]){
  const n=node('notification',{id:'D1',from:'200:1@s.whatsapp.net',type:'devices'},[node(tag,{device_hash:'HASH'},list.map(copy))]),h=makeDevices(seed,failDelete);let error;try{await h.handleDevicesNotification(n)}catch(e){error=e.message}
  deviceCases.push({seed,failDelete,node:wireNode(n),cache:Object.fromEntries([...h.cache].map(([k,v])=>[k,v.map(e=>({user:e.user,server:e.server,device:e.device??null}))])),deleted:h.deleted,error:error!==undefined})
 }
 for(const content of [undefined,[],[node('add',{})]]){const n=node('notification',{id:'D2',from:'100@s.whatsapp.net',type:'devices'},content),h=makeDevices(seeds[1],false);await h.handleDevicesNotification(n);deviceCases.push({seed:seeds[1],failDelete:false,node:wireNode(n),cache:Object.fromEntries(h.cache),deleted:h.deleted,error:false})}
 await writeFile('tests/fixtures/notifications.json',JSON.stringify({me:'100:7@s.whatsapp.net',lid:'900:7@lid',cases,deviceCases},null,1)+'\n');console.log(`Generated ${cases.length} original notification handler cases and ${deviceCases.length} device-list cases`)
}finally{await rm(temp,{recursive:true,force:true})}
