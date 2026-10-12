// Execute the original MIT chats.ts profile, block, business-profile, dirty-bit and call-link operations with I/O stubs.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-chat-ops-'))
try{
 for(const name of ['constants','jid-utils','generic-utils']){
  let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
  s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}")
  s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
  await writeFile(join(temp,`${name}.mjs`),s)
 }
 const chats=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8')
 const slice=(from,to)=>{const a=chats.indexOf(from),b=chats.indexOf(to,a);if(a<0||b<0)throw Error('upstream slice not found: '+from);return chats.slice(a,b)}
 const content=slice('export const buildProfilePictureQueryContent','export const makeChatsSocket').replace('export const','const')
 const operations=slice('const updateDefaultDisappearingMode','const newAppStateChunkHandler')
 const picture=slice('const profilePictureUrl','const sendPresenceUpdate')
 const dirty=slice("ws.on('CB:ib,,dirty'","ev.on('connection.update'")
 const factory=`export function make(opts={}){const queries=[],sent=[],events=[],mods=[],usync=[],responses=[...(opts.responses??[])];let tag=0;
 const authState={creds:{me:{id:'100:7@s.whatsapp.net',lid:'900:7@lid'},lastAccountSyncTimestamp:opts.lastSync}},S_WHATSAPP_NET='@s.whatsapp.net',logger={info(){},debug(){},warn(){},error(){}};
 const query=async(node,timeout)=>{queries.push({node,timeout:timeout??null});return responses.shift()??{tag:'iq',attrs:{}}},sendNode=async node=>{sent.push(node)},generateMessageTag=()=>'TAG-'+(++tag);
 const serverProps={profilePicPrivacyToken:opts.pictureToken??true},signalRepository={lidMapping:{getPNForLID:async j=>opts.pn?.[j],getLIDForPN:async j=>opts.lid?.[j]}},getLIDForPN=signalRepository.lidMapping.getLIDForPN;
 const chatModify=async(mod,jid)=>{mods.push({mod,jid})},generateProfilePicture=async(c,d)=>({img:Buffer.from('IMG:'+c+':'+JSON.stringify(d??null))}),buildTcTokenFromJid=async({jid})=>opts.token?.[jid];
 class USyncQuery{constructor(){this.protocols=[];this.users=[]}withStatusProtocol(){this.protocols.push('status');return this}withDisappearingModeProtocol(){this.protocols.push('disappearing_mode');return this}withUser(u){this.users.push(u.id);return this}}
 class USyncUser{withId(id){this.id=id;return this}}
 const sock={executeUSyncQuery:async q=>{usync.push({protocols:q.protocols,users:q.users});return opts.usyncResult}},ev={emit:(name,data)=>events.push({name,data})},handlers={},ws={on:(key,fn)=>{handlers[key]=fn}};
 `
 await writeFile(join(temp,'ops.mjs'),`import {getBinaryNodeChild,getBinaryNodeChildren} from './generic-utils.mjs';import {isHostedLidUser,isHostedPnUser,isLidUser,isPnUser,jidNormalizedUser} from './jid-utils.mjs';class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}`+stripTypeScriptTypes(content+factory+operations+picture+dirty+`return {ops:{updateDefaultDisappearingMode,getBotListV2,fetchStatus,fetchDisappearingDuration,updateProfilePicture,removeProfilePicture,updateProfileStatus,updateProfileName,fetchBlocklist,updateBlockStatus,getBusinessProfile,cleanDirtyBits,profilePictureUrl,createCallLink,dirty:handlers['CB:ib,,dirty']},queries,sent,events,mods,usync}}`))
 const {make}=await import(pathToFileURL(join(temp,'ops.mjs')))
 const node=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),cases=[]
 const wire=n=>n===undefined?null:({tag:n.tag,attrs:Object.fromEntries(Object.entries(n.attrs).filter(([,v])=>v!==undefined)),...(n.content===undefined?{}:{content:Array.isArray(n.content)?n.content.map(wire):Buffer.isBuffer(n.content)?{type:'Buffer',data:n.content.toString('base64')}:n.content})})
 const copy=v=>Buffer.isBuffer(v)?Buffer.from(v):Array.isArray(v)?v.map(copy):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).map(([k,x])=>[k,copy(x)])):v
 const plain=v=>Buffer.isBuffer(v)?{type:'Buffer',data:v.toString('base64')}:Array.isArray(v)?v.map(plain):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,x])=>x!==undefined).map(([k,x])=>[k,plain(x)])):v
 async function run(op,args,opts={}){const h=make(copy(opts));let result,error;try{result=await h.ops[op](...copy(args))}catch(e){error={message:e.message,status:e.output?.statusCode??null}}
  cases.push({op,args:plain(args),opts:plain(opts),queries:h.queries.map(q=>({node:wire(q.node),timeout:q.timeout})),sent:h.sent.map(wire),events:h.events,mods:h.mods,usync:h.usync,result:result===undefined?null:result,error:error??null})}
 for(const d of [0,86400,7776000])await run('updateDefaultDisappearingMode',[d])
 await run('getBotListV2',[],{responses:[node('iq',{},[node('bot',{},[node('section',{type:'all'},[node('bot',{jid:'13135550002@c.us',persona_id:'P1'}),node('bot',{jid:'867051314767696@bot'})]),node('section',{type:'featured'},[node('bot',{jid:'x@bot',persona_id:'P9'})]),node('section',{type:'all'},[node('bot',{persona_id:'P3'})])])])]})
 await run('getBotListV2',[],{responses:[node('iq',{})]});await run('getBotListV2',[],{responses:[node('iq',{},[node('bot',{})])]})
 for(const op of ['fetchStatus','fetchDisappearingDuration']){await run(op,['200@s.whatsapp.net','300@lid'],{usyncResult:{list:[{id:'200@s.whatsapp.net'}],sideList:[]}});await run(op,[]);await run(op,['200@s.whatsapp.net'])}
 for(const jid of ['100:7@s.whatsapp.net','100@s.whatsapp.net','100@c.us','900@lid','group@g.us','200:3@s.whatsapp.net','','invalid']){await run('updateProfilePicture',[jid,'photo']);await run('updateProfilePicture',[jid,'photo',{width:96,height:96}]);await run('removeProfilePicture',[jid])}
 for(const status of ['Busy ☕','',' spaced '])await run('updateProfileStatus',[status])
 for(const name of ['Name ☕',''])await run('updateProfileName',[name])
 for(const response of [node('iq',{},[node('list',{},[node('item',{jid:'200@s.whatsapp.net'}),node('item',{}),node('other',{jid:'x'}),node('item',{jid:'201@lid'})])]),node('iq',{}),node('iq',{},[node('list',{})])])await run('fetchBlocklist',[],{responses:[response]})
 const maps={pn:{'201@lid':'201:5@s.whatsapp.net','401@hosted.lid':'401@hosted'},lid:{'200@s.whatsapp.net':'2000@lid','400@hosted':'4000@hosted.lid'}}
 for(const jid of ['200@s.whatsapp.net','200:4@s.whatsapp.net','200@c.us','201@lid','201:9@lid','300@s.whatsapp.net','301@lid','400@hosted','401@hosted.lid','group@g.us','status@broadcast','','invalid'])for(const action of ['block','unblock']){await run('updateBlockStatus',[jid,action],maps);await run('updateBlockStatus',[jid,action])}
 const text=(tag,value)=>node(tag,{},Buffer.from(value))
 const profiles=[node('iq',{},[node('business_profile',{},[node('profile',{jid:'500@s.whatsapp.net'},[text('address','1 Road ☕'),text('description','Shop'),text('website','https://example.com'),node('website',{},'ignored'),text('email','a@example.com'),node('categories',{},[text('category','Cafe'),text('category','Other')]),node('business_hours',{timezone:'Europe/Kyiv'},[node('business_hours_config',{day_of_week:'mon',mode:'open_24h'}),node('business_hours_config',{day_of_week:'tue',mode:'specific_hours',open_time:'480',close_time:'1020'}),node('other',{})])])])]),
  node('iq',{},[node('business_profile',{},[node('profile',{},[text('description',''),node('website',{},''),node('address',{},'plain')])])]),node('iq',{},[node('business_profile',{},[node('profile',{jid:'501@s.whatsapp.net'})])]),node('iq',{},[node('business_profile',{})]),node('iq',{})]
 for(const response of profiles)await run('getBusinessProfile',['500@s.whatsapp.net'],{responses:[response]})
 for(const args of [['account_sync'],['groups'],['account_sync',1700000000],['account_sync','1700000001'],['groups',0],['groups','']])await run('cleanDirtyBits',args)
 const tokens={token:{'200@s.whatsapp.net':[node('tctoken',{t:'1700000000'},Buffer.from([1,2,3]))],'300@lid':[]}}
 const url=node('iq',{},[node('picture',{url:'https://pps.example/p.jpg',id:'1',type:'image',direct_path:'/p'})])
 for(const jid of ['200@s.whatsapp.net','200:3@s.whatsapp.net','200@c.us','300@lid','400@s.whatsapp.net','100:7@s.whatsapp.net','900@lid','group@g.us','news@newsletter'])for(const args of [[jid],[jid,'image'],[jid,'preview',5000]]){await run('profilePictureUrl',args,{...tokens,responses:[url]});await run('profilePictureUrl',args,{...tokens,pictureToken:false,responses:[node('iq',{},[node('picture',{})])]})}
 await run('profilePictureUrl',['200@s.whatsapp.net'],{responses:[node('iq',{})]})
 for(const args of [['audio'],['video'],['video',{startTime:1700000000}],['audio',{startTime:0},2500],['video',undefined,100]])await run('createCallLink',args,{responses:[node('call',{},[node('link_create',{token:'TOKEN'})])]})
 await run('createCallLink',['audio'],{responses:[node('call',{})]});await run('createCallLink',['audio'],{responses:[node('call',{},[node('link_create',{})])]})
 const ib=attrs=>node('ib',{from:'@s.whatsapp.net'},[node('dirty',attrs)])
 for(const lastSync of [undefined,0,1699999999])for(const attrs of [{type:'account_sync',timestamp:'1700000002'},{type:'account_sync'},{type:'account_sync',timestamp:''},{type:'account_sync',timestamp:'bad'},{type:'groups',timestamp:'1700000003'},{type:'other',timestamp:'1'},{}])await run('dirty',[ib(attrs)],{lastSync})
 await writeFile('tests/fixtures/chat_operations.json',JSON.stringify({me:'100:7@s.whatsapp.net',lid:'900:7@lid',cases},null,1)+'\n');console.log(`Generated ${cases.length} original chat operation cases`)
}finally{await rm(temp,{recursive:true,force:true})}
