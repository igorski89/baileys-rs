// Run the MIT-licensed Baileys newsletter socket, MEX query helper and newsletter/MEX notification handlers with stubs.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-newsletters-')),hex=b=>Buffer.from(b).toString('hex')
const NOW=1700000000000;Date.now=()=>NOW
// Dates are recorded as epoch milliseconds (null when invalid); undefined fields are dropped.
const clean=v=>v instanceof Date?{$date:Number.isNaN(v.getTime())?null:v.getTime()}:Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(x=>x===undefined?null:clean(x)):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):typeof v==='number'&&!Number.isFinite(v)?null:v
const enums=s=>s.replace(/export enum (\w+) \{([^}]+)\}/g,(_,name,body)=>`export const ${name} = {${body.replaceAll(' = ',': ')}}`)
try {
  const boom=JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)
  for(const name of ['constants','jid-utils','encode','generic-utils']){
    let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
    s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("'@hapi/boom'",boom)
    s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
    await writeFile(join(temp,`${name}.mjs`),s)
  }
  await writeFile(join(temp,'proto.mjs'),(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
  const read=path=>readFile(join(upstream,path),'utf8')
  const types=stripTypeScriptTypes(enums(await read('src/Types/Mex.ts')))
  const state=await read('src/Types/State.ts'),enforcement=stripTypeScriptTypes(enums(state.slice(state.indexOf('export enum ReachoutTimelockEnforcementType'),state.indexOf('export enum NewChatMessageCappingStatusType'))))
  await writeFile(join(temp,'types.mjs'),types+enforcement)
  await writeFile(join(temp,'mex.mjs'),stripTypeScriptTypes(await read('src/Socket/mex.ts')).replace("'@hapi/boom'",boom).replace(/import \{ getBinaryNodeChild, S_WHATSAPP_NET \} from '\.\.\/WABinary'/,"import {getBinaryNodeChild} from './generic-utils.mjs';import {S_WHATSAPP_NET} from './jid-utils.mjs'"))
  const newsletter=await read('src/Socket/newsletter.ts'),core=await read('src/Socket/socket.ts'),recv=await read('src/Socket/messages-recv.ts')
  const fetches=core.slice(core.indexOf('\tconst fetchAccountReachoutTimelock = async'),core.indexOf("\treturn {\n\t\ttype: 'md'"))
  if(!fetches.length)throw Error('upstream socket slice not found')
  await writeFile(join(temp,'newsletter.mjs'),`import {getBinaryNodeChild} from './generic-utils.mjs';import {QueryIds,XWAPaths,ReachoutTimelockEnforcementType} from './types.mjs';import {executeWMexQuery as genericExecuteWMexQuery} from './mex.mjs';const executeWMexQuery=genericExecuteWMexQuery,makeGroupsSocket=c=>c.sock,generateProfilePicture=async()=>({img:Buffer.from('synthetic-jpeg')});`
    +stripTypeScriptTypes(newsletter.slice(newsletter.indexOf('const parseNewsletterCreateResponse'))+`export function makeCore({query,generateMessageTag,ev}){${fetches}return {fetchAccountReachoutTimelock,fetchNewChatMessageCap}}`))
  const {makeNewsletterSocket,makeCore}=await import(pathToFileURL(join(temp,'newsletter.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')))
  const n=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),iq=content=>n('iq',{type:'result'},content)
  const raw=text=>iq([n('result',{},Buffer.from(text))]),mex=body=>raw(JSON.stringify(body)),data=(path,value)=>mex({data:{[path]:value}})
  const jid='120363000000000000@newsletter'
  const created={id:jid,state:{type:'ACTIVE'},thread_metadata:{creation_time:'1700000001',description:{id:'1',text:'Description 💚',update_time:'1'},handle:null,invite:'INVITE',name:{id:'2',text:'Name 💚',update_time:'2'},picture:{direct_path:'/v/pic',id:'PIC',type:'IMAGE'},preview:{direct_path:'/v/pre',id:'PRE',type:'PREVIEW'},subscribers_count:'12',verification:'UNVERIFIED'},viewer_metadata:{mute:'OFF',role:'OWNER'}}
  const thread=t=>({...created,thread_metadata:{...created.thread_metadata,...t}})
  const gql=errors=>mex({errors,data:{xwa2_newsletter_create:created}})
  const calls=[
    ['newsletterCreate',['Name 💚','Description "quoted" 💚'],[data('xwa2_newsletter_create',created)]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',thread({creation_time:'abc',subscribers_count:undefined,invite:undefined,verification:undefined,picture:{}}))]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',thread({creation_time:' 42px',subscribers_count:'-7.9',name:{},description:{}}))]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',{id:jid,viewer_metadata:{mute:'ON'}})]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',thread({name:null}))]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',thread({picture:null}))]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',{...created,viewer_metadata:null})]],
    ['newsletterCreate',['Name'],[data('xwa2_newsletter_create',null)]],
    ['newsletterCreate',['Name'],[gql([{message:'Not allowed',extensions:{error_code:404}},{},{message:'Second'}])]],
    ['newsletterCreate',['Name'],[gql([{message:'Zero code',extensions:{error_code:0}}])]],
    ['newsletterCreate',['Name'],[gql([{message:'Text code',extensions:{error_code:'422'}}])]],
    ['newsletterCreate',['Name'],[gql([{message:'Low code',extensions:{error_code:200}}])]],
    ['newsletterCreate',['Name'],[gql([{message:'Odd code',extensions:{error_code:'odd'}}])]],
    ['newsletterCreate',['Name'],[gql([{extensions:null}])]],
    ['newsletterCreate',['Name'],[gql([{message:''}])]],
    ['newsletterCreate',['Name'],[gql([null])]],
    ['newsletterCreate',['Name'],[gql('failure')]],
    ['newsletterCreate',['Name'],[gql([])]],
    ['newsletterCreate',['Name'],[mex({data:{other:1}})]],
    ['newsletterCreate',['Name'],[mex({data:null})]],
    ['newsletterCreate',['Name'],[mex({})]],
    ['newsletterCreate',['Name'],[mex(null)]],
    ['newsletterCreate',['Name'],[mex(5)]],
    ['newsletterCreate',['Name'],[raw('{not json')]],
    ['newsletterCreate',['Name'],[raw('')]],
    ['newsletterCreate',['Name'],[iq([n('result',{},[])])]],
    ['newsletterCreate',['Name'],[iq([n('result',{})])]],
    ['newsletterFollow',[jid],[iq([n('result',{},JSON.stringify({data:{xwa2_newsletter_join_v2:{ok:true}}}))])]],
    ['newsletterCreate',['Name'],[iq([])]],
    ['newsletterUpdate',[jid,{name:'N',description:'D',picture:'cGlj'}],[data('xwa2_newsletter_update',{id:jid})]],
    ['newsletterUpdate',[jid,{}],[data('xwa2_newsletter_update',null)]],
    ['newsletterSubscribers',[jid],[data('xwa2_newsletter_subscribers',{subscribers:12})]],
    ['newsletterSubscribers',[jid],[mex({data:{}})]],
    ...[{id:jid,thread_metadata:{name:{text:'N'}}},{result:{id:jid,state:{type:'ACTIVE'}}},{result:{id:null}},{result:null},{result:{name:'x'}},{id:5},{id:5,result:{id:jid}},null,[],'text',7].map(value=>['newsletterMetadata',['jid',jid],[data('xwa2_newsletter',value)]]),
    ['newsletterMetadata',['invite','INVITE'],[data('xwa2_newsletter',{id:jid})]],
    ['newsletterMetadata',['invite','INVITE'],[mex({data:{xwa2_newsletter_metadata:{id:jid}}})]],
    ...['newsletterFollow','newsletterUnfollow','newsletterMute','newsletterUnmute'].flatMap(method=>{const path={newsletterFollow:'xwa2_newsletter_join_v2',newsletterUnfollow:'xwa2_newsletter_leave_v2',newsletterMute:'xwa2_newsletter_mute_v2',newsletterUnmute:'xwa2_newsletter_unmute_v2'}[method];return [[method,[jid],[data(path,{id:jid,ok:true})]],[method,[jid],[data(path,null)]],[method,[jid],[mex({data:{}})]]]}),
    ['newsletterUpdateName',[jid,'New name 💚'],[data('xwa2_newsletter_update',{id:jid})]],
    ['newsletterUpdateDescription',[jid,'New description\nline'],[data('xwa2_newsletter_update',{id:jid})]],
    ['newsletterUpdateDescription',[jid,''],[data('xwa2_newsletter_update',{id:jid})]],
    ['newsletterRemovePicture',[jid],[data('xwa2_newsletter_update',{id:jid})]],
    ['newsletterReactMessage',[jid,'175','👍'],[n('ack',{class:'message'})]],
    ['newsletterReactMessage',[jid,'175'],[n('ack',{class:'message'})]],
    ['newsletterReactMessage',[jid,'175',''],[n('ack',{class:'message'})]],
    ['newsletterFetchMessages',[jid,10,1700000000,175],[iq([n('messages',{},[n('message',{server_id:'176'})])])]],
    ['newsletterFetchMessages',[jid,5,0,0],[iq([])]],
    ['newsletterFetchMessages',[jid,1],[iq([])]],
    ['subscribeNewsletterUpdates',[jid],[iq([n('live_updates',{duration:'300'})])]],
    ['subscribeNewsletterUpdates',[jid],[iq([n('live_updates',{duration:''})])]],
    ['subscribeNewsletterUpdates',[jid],[iq([n('live_updates',{})])]],
    ['subscribeNewsletterUpdates',[jid],[iq([])]],
    ['newsletterAdminCount',[jid],[data('xwa2_newsletter_admin',{admin_count:3})]],
    ['newsletterAdminCount',[jid],[data('xwa2_newsletter_admin',{})]],
    ['newsletterAdminCount',[jid],[data('xwa2_newsletter_admin',7)]],
    ['newsletterAdminCount',[jid],[data('xwa2_newsletter_admin',null)]],
    ['newsletterAdminCount',[jid],[mex({data:{}})]],
    ['newsletterChangeOwner',[jid,'300@lid'],[data('xwa2_newsletter_change_owner',{id:jid})]],
    ['newsletterChangeOwner',[jid,'300@lid'],[mex({data:{}})]],
    ['newsletterDemote',[jid,'300@lid'],[data('xwa2_newsletter_demote',{id:jid})]],
    ['newsletterDelete',[jid],[data('xwa2_newsletter_delete_v2',{id:jid})]],
    ['newsletterDelete',[jid],[gql([{message:'Forbidden',extensions:{error_code:403}}])]],
    ...[{is_active:true,time_enforcement_ends:'1700000100',enforcement_type:'BIZ_QUALITY'},{is_active:false,time_enforcement_ends:'0',enforcement_type:'DEFAULT'},{is_active:1,time_enforcement_ends:'bad',enforcement_type:'FUTURE_TYPE'},{time_enforcement_ends:''},{is_active:'',enforcement_type:null,time_enforcement_ends:1700000200},{},null].map(value=>['fetchAccountReachoutTimelock',[],[data('xwa2_fetch_account_reachout_timelock',value)]]),
    ['fetchAccountReachoutTimelock',[],[mex({data:{}})]],
    ['fetchNewChatMessageCap',[],[data('xwa2_message_capping_info',{total_quota:100,used_quota:3,cycle_start_timestamp:'1700000000',capping_status:'NONE'})]],
    ['fetchNewChatMessageCap',[],[mex({errors:[{message:'Nope'}]})]]
  ]
  const operations=[]
  for(const [method,args,responses] of calls){const requests=[],events=[],queue=[...responses];let counter=0
    const query=async node=>{requests.push(node);return queue.shift()},generateMessageTag=()=>'TAG'+(counter++),ev={emit:(name,data)=>events.push({name,data})}
    const sock={...makeCore({query,generateMessageTag,ev}),query,generateMessageTag,ev}
    let expected,error;try{expected=await makeNewsletterSocket({sock})[method](...args)}catch(e){error=e}
    operations.push({method,args,requests:requests.map(n=>hex(encodeBinaryNode(n))),responses:responses.map(n=>hex(encodeBinaryNode(n))),expected:clean(expected)??null,error:error!==undefined,...(error?.isBoom?{statusCode:error.output.statusCode,message:error.message,data:clean(error.data)??null}:{}),events:clean(events)})
  }
  // Notification handlers, sliced from the receive socket.
  const handlers=recv.slice(recv.indexOf('\tconst handleMexNotification ='),recv.indexOf('\tconst sendMessageAck ='))
  const valid=recv.slice(recv.indexOf('const ENFORCEMENT_TYPE_VALUES'),recv.indexOf('export const makeMessagesRecvSocket'))
  if(!handlers.length||!valid.length)throw Error('upstream notification slice not found')
  await writeFile(join(temp,'handlers.mjs'),`import {getAllBinaryNodeChildren,getBinaryNodeChild,getBinaryNodeChildString,binaryNodeToString} from './generic-utils.mjs';import {ReachoutTimelockEnforcementType} from './types.mjs';import {proto} from './proto.mjs';`
    +stripTypeScriptTypes(valid+`export function make(){const events=[],upserts=[],stores=[];const ev={emit(name,data){events.push({name,data})}},logger={debug(){},error(){},info(){},warn(){}},signalRepository={lidMapping:{storeLIDPNMappings:async m=>{stores.push(m.map(x=>({...x})))}}};const upsertMessage=async(m,kind)=>upserts.push({kind,wire:Buffer.from(proto.WebMessageInfo.encode(m).finish()).toString('hex')});${handlers}return {handleNewsletterNotification,handleMexNotification,events,upserts,stores}}`))
  const {make}=await import(pathToFileURL(join(temp,'handlers.mjs')))
  const wireNode=n=>({tag:n.tag,attrs:n.attrs,...(n.content===undefined?{}:{content:Array.isArray(n.content)?n.content.map(wireNode):Buffer.isBuffer(n.content)?{type:'Buffer',data:n.content.toString('base64')}:n.content})})
  const notifications=[]
  async function run(node){const h=make(),input=wireNode(node);let error=false;try{await (node.attrs.type==='mex'?h.handleMexNotification:h.handleNewsletterNotification)(node)}catch(e){error=true}
    notifications.push({node:input,events:clean(h.events),upserts:h.upserts,stores:clean(h.stores),error})}
  const b=text=>Buffer.from(text),root={id:'NL1',from:jid,type:'newsletter',participant:'300@lid',t:'1700000000'},bare={id:'NL2',type:'newsletter'}
  const encode=m=>Buffer.from(proto.Message.encode(proto.Message.fromObject(m)).finish())
  const messages=[{conversation:'Plain newsletter text 💚'},{extendedTextMessage:{text:'Link https://example.com',matchedText:'https://example.com',contextInfo:{forwardingScore:3,isForwarded:true,forwardedNewsletterMessageInfo:{newsletterJid:jid,serverMessageId:175,newsletterName:'News'}}}},{imageMessage:{url:'https://mmg.whatsapp.net/v/img',mimetype:'image/jpeg',caption:'Caption',fileSha256:Buffer.alloc(32,7),fileLength:'123456789012',height:100,width:200,mediaKeyTimestamp:'1700000000',jpegThumbnail:Buffer.from([255,216,0,1])}},{pollCreationMessageV3:{name:'Poll',options:[{optionName:'A'},{optionName:'B'}],selectableOptionsCount:1}},{}].map(encode)
  const children=[
    n('reaction',{message_id:'175'},[n('reaction',{},b('👍'))]),n('reaction',{message_id:'175'},[n('reaction',{},'❤')]),n('reaction',{message_id:'175'},[n('reaction',{})]),n('reaction',{},[n('reaction',{},[n('x')])]),n('reaction',{message_id:''}),
    n('view',{message_id:'175'},b('42')),n('view',{message_id:'175'},'7'),n('view',{message_id:'175'},b('12abc')),n('view',{message_id:'175'},b('abc')),n('view',{}),n('view',{message_id:'175'},[]),n('view',{message_id:'175'},[n('x')]),n('view',{message_id:'175'},b('')),n('view',{message_id:'175'},b('-3')),
    n('participant',{jid:'400@lid',action:'promote',role:'ADMIN'}),n('participant',{}),n('participant',{jid:'',action:'demote',role:''}),
    n('update',{},[n('settings',{},[n('name',{},b('New name 💚')),n('description',{},b('New description'))])]),n('update',{},[n('settings',{},[n('name',{},'String name'),n('description',{},'')])]),n('update',{},[n('settings',{},[n('name',{},b('')),n('description',{})])]),n('update',{},[n('settings',{},[n('name',{},[n('x'),n('y')]),n('description',{},[])])]),n('update',{},[n('settings',{})]),n('update',{},[n('other',{})]),n('update',{}),
    ...messages.map((m,i)=>n('message',{message_id:'M'+i,server_id:'17'+i,t:'1700000000'},[n('plaintext',{},m)])),
    n('message',{server_id:'180',t:'1700000001'},[n('plaintext',{},messages[0])]),n('message',{message_id:'',server_id:'181',t:'1700000001'},[n('plaintext',{},messages[0])]),n('message',{message_id:'',server_id:'',t:'1700000001'},[n('plaintext',{},messages[0])]),n('message',{t:'1700000001'},[n('plaintext',{},messages[0])]),
    ...[undefined,'','bad','1.9','-5','18446744073709551616'].map(t=>n('message',{message_id:'T',t},[n('plaintext',{},messages[0])])),
    n('message',{message_id:'S',t:'1'},[n('plaintext',{},encode({conversation:'ascii via string'}).toString('binary'))]),n('message',{message_id:'S2',t:'1'},[n('plaintext',{},messages[2].toString('binary'))]),
    n('message',{message_id:'E',t:'1'},[n('plaintext',{},b(''))]),n('message',{message_id:'E',t:'1'},[n('plaintext',{},'')]),n('message',{message_id:'E',t:'1'},[n('plaintext',{})]),n('message',{message_id:'E',t:'1'}),n('message',{message_id:'E',t:'1'},[n('plaintext',{},[])]),n('message',{message_id:'E',t:'1'},[n('plaintext',{},[n('x')])]),n('message',{message_id:'E',t:'1'},[n('plaintext',{},[n('x'),n('y')])]),
    // protobufjs accepts a string truncated at the end of the buffer; that leniency is not reproduced.
    n('message',{message_id:'B',t:'1'},[n('plaintext',{},Buffer.from([0xff,0xff,0xff]))]),
    n('unknown',{})
  ]
  const copy=x=>({tag:x.tag,attrs:{...x.attrs},...(x.content===undefined?{}:{content:Array.isArray(x.content)?x.content.map(copy):x.content})})
  for(const attrs of [root,bare])for(const child of children)await run(n('notification',attrs,[copy(child)]))
  await run(n('notification',root,children.slice(0,20).map(copy)));await run(n('notification',root));await run(n('notification',root,[]))
  const mroot={id:'MX1',from:'s.whatsapp.net',type:'mex',t:'1700000000'},update=(op,body,tag='update')=>n(tag,op===undefined?{}:{op_name:op},typeof body==='string'||Buffer.isBuffer(body)||Array.isArray(body)||body===undefined?body:b(JSON.stringify(body)))
  const lock=payload=>({data:{xwa2_notify_account_reachout_timelock:payload}}),LOCK='NotificationUserReachoutTimelockUpdate',CAP='MessageCappingInfoNotification',NU='NotificationNewsletterUpdate',PROMOTE='NotificationNewsletterAdminPromote',LINKED='NotificationLinkedProfilesUpdates'
  const settings=[{jid,settings:{name:{text:'Renamed 💚'},description:{text:'D'}}},{jid,settings:{}},{settings:{name:{text:'no jid'}}},{jid,settings:'text'},{jid,settings:['a']},{jid,settings:[]},{jid,settings:5},{jid:'',settings:{a:1}},{jid},7,'s']
  const promotes=[{jid,user:'400@lid'},{jid},{user:'400@lid'},{jid:'',user:'400@lid'},{jid,user:''},{}]
  const profiles=[{jid:'900@lid',added_profiles:['200@s.whatsapp.net',{pn:'201@s.whatsapp.net'},{jid:'202@s.whatsapp.net'},{pn:null,jid:'203@s.whatsapp.net'},{pn:'',jid:'204@s.whatsapp.net'},{},'',null,5]},{added_profiles:['205@s.whatsapp.net']},{jid:'901@lid'},{jid:'902@lid',added_profiles:'206@s.whatsapp.net'},null,{jid:'903@lid',added_profiles:[]}]
  const mexChildren=[
    ...[{is_active:true,time_enforcement_ends:'1700000500',enforcement_type:'BIZ_QUALITY'},{is_active:true},{is_active:true,enforcement_type:'FUTURE_TYPE',time_enforcement_ends:'bad'},{is_active:true,enforcement_type:5,time_enforcement_ends:1700000600},{is_active:'yes',enforcement_type:'WEB_COMPANION_ONLY',time_enforcement_ends:''},{is_active:false,enforcement_type:'BIZ_QUALITY',time_enforcement_ends:'1700000500'},{},null,0].map(p=>[update(LOCK,lock(p))]),
    [update(LOCK,{data:{}})],[update(LOCK,{data:null})],[update(LOCK,{})],[update(LOCK,{data:5})],[update(LOCK,{...lock({is_active:true}),errors:[{message:'x'}]})],[update(LOCK,{...lock({is_active:true}),errors:[]})],[update(LOCK,{...lock({is_active:true}),errors:'text'})],[update(LOCK,{...lock({is_active:true}),errors:{length:2}})],[update(LOCK,{...lock({is_active:true}),errors:{}})],
    [update(LOCK,'{not json')],[update(LOCK,undefined)],[update(LOCK,'null')],[update(LOCK,'5')],[update(LOCK,[])],[update(LOCK,JSON.stringify(lock({is_active:true})))],
    [update(CAP,{data:{xwa2_notify_new_chat_messages_capping_info_update:{total_quota:100,used_quota:99,capping_status:'SECOND_WARNING',cycle_end_timestamp:'1700009999'}}})],[update(CAP,{data:{xwa2_notify_new_chat_messages_capping_info_update:null}})],[update(CAP,{data:{other:1}})],
    [update('NotificationSomethingNew',{data:{x:1},operation:NU,updates:settings})],
    // Legacy payloads: no op_name, a <mex> child, or any first child.
    [update(undefined,{operation:NU,updates:settings})],[update('',{operation:NU,updates:settings})],[update(NU,{data:{x:1},updates:settings})],[update(NU,{updates:settings})],[update(NU,{data:{x:1},operation:PROMOTE,updates:promotes})],
    [update(undefined,{operation:NU,updates:settings},'mex')],[update(undefined,JSON.stringify({operation:PROMOTE,updates:promotes}),'mex')],[update(undefined,{operation:PROMOTE,updates:promotes},'mex'),update(LOCK,lock({is_active:true}))],
    [update(undefined,'','mex'),update(undefined,{operation:PROMOTE,updates:promotes})],[update(undefined,undefined,'mex'),update(undefined,{operation:PROMOTE,updates:promotes},'other')],[update(undefined,{operation:PROMOTE,updates:promotes},'other')],[update(PROMOTE,{updates:promotes},'other')],
    [update(undefined,[],'mex')],[update(undefined,[n('x')],'mex')],[update(undefined,b(''),'mex')],[update(undefined,'{bad','mex')],[update(undefined,'null','mex')],[update(undefined,'7','mex')],[update(undefined,undefined)],
    [update(undefined,{operation:PROMOTE})],[update(undefined,{updates:promotes})],[update(undefined,{operation:null,updates:promotes})],[update(undefined,{operation:'Unknown',updates:{}})],[update(undefined,{operation:5,updates:promotes})],
    ...[NU,PROMOTE,LINKED].flatMap(op=>[[update(undefined,{operation:op,updates:[null]})],[update(undefined,{operation:op,updates:{a:1}})],[update(undefined,{operation:op,updates:'text'})],[update(undefined,{operation:op,updates:7})],[update(undefined,{operation:op,updates:[]})]]),
    [update(undefined,{operation:LINKED,updates:profiles})],[update(LINKED,{data:{xwa2_notify_linked_profiles:profiles[0]}})],[update(undefined,{operation:LINKED,data:{xwa2_notify_linked_profiles:profiles[0]}},'mex')],[update(LINKED,{data:{xwa2_notify_linked_profiles:null}})],[update(LINKED,{data:{xwa2_notify_linked_profiles:profiles[0]},updates:[profiles[1]]})],
    [],undefined
  ]
  for(const content of mexChildren)await run(n('notification',mroot,content?.map(copy)))
  await run(n('notification',{id:'MX2',type:'mex'},[update(undefined,{operation:PROMOTE,updates:promotes})]))
  await writeFile('tests/fixtures/newsletters.json',JSON.stringify({now:NOW,operations,notifications},null,1)+'\n')
  console.log(`Generated ${operations.length} original newsletter/MEX operations and ${notifications.length} notification cases`)
}finally{await rm(temp,{recursive:true,force:true})}
