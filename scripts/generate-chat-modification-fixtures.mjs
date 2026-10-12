// Execute the original MIT chatModificationToAppPatch with a fixed clock and protobufjs action encoding.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-chat-modify-'))
try{
 await writeFile(join(temp,'proto.mjs'),(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
 const source=await readFile(join(upstream,'src/Utils/chat-utils.ts'),'utf8'),jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8')
 let jidSource=jid.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
 await writeFile(join(temp,'jid-utils.mjs'),stripTypeScriptTypes(jidSource))
 const body=source.slice(source.indexOf('export const chatModificationToAppPatch'),source.indexOf('export const processSyncAction'))
 if(!body.length)throw Error('upstream slice not found')
 await writeFile(join(temp,'modify.mjs'),`import {proto} from './proto.mjs';import {isJidGroup,jidNormalizedUser} from './jid-utils.mjs';const LabelAssociationType={Chat:'label_jid',Message:'label_message'},toNumber=t=>typeof t==='object'&&t?('toNumber'in t?t.toNumber():t.low):t||0;class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}`+stripTypeScriptTypes(body))
 const {chatModificationToAppPatch}=await import(pathToFileURL(join(temp,'modify.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')))
 const NOW=1700000000123,cases=[],original=Date.now
 const run=(mod,jid)=>{const input=structuredClone(mod);Date.now=()=>NOW;let expected,error;try{const p=chatModificationToAppPatch(mod,jid);expected={type:p.type,index:p.index,apiVersion:p.apiVersion,operation:p.operation,action:Buffer.from(proto.SyncActionValue.encode(p.syncAction).finish()).toString('hex')}}catch(e){error={message:e.message,status:e.output?.statusCode??null}}finally{Date.now=original}cases.push({mod:input,jid,expected,error})}
 const m=(id,ts,extra={})=>({key:{remoteJid:'200@s.whatsapp.net',id,fromMe:false,...extra},messageTimestamp:ts})
 const lists=[undefined,[],[m('A',1700000000)],[m('A',1699999990,{fromMe:true}),m('B',1700000001)],[{key:{remoteJid:'group@g.us',id:'G',fromMe:false,participant:'300:4@s.whatsapp.net'},messageTimestamp:1700000002}],[{key:{remoteJid:'group@g.us',id:'G',fromMe:true},messageTimestamp:1700000002}],
  [{key:{remoteJid:'group@g.us',id:'G',fromMe:false},messageTimestamp:1700000002}],[{key:{id:'NOJID'},messageTimestamp:5}],[{key:{remoteJid:'200@s.whatsapp.net'},messageTimestamp:5}],[m('A',0)],[{key:{remoteJid:'200@s.whatsapp.net',id:'A'}}],
  {lastMessageTimestamp:1700000003,lastSystemMessageTimestamp:1700000004,messages:[{key:{remoteJid:'200@s.whatsapp.net',id:'R',fromMe:true},timestamp:1700000003}]},{}]
 const jids=['200@s.whatsapp.net','group@g.us','300@lid']
 for(const jid of jids){
  for(const mute of [null,0,1700003600000,8640000000000])run({mute},jid)
  for(const lastMessages of lists){for(const archive of [true,false])run({archive,lastMessages},jid);for(const markRead of [true,false])run({markRead,lastMessages},jid);run({clear:true,lastMessages},jid);run({delete:true,lastMessages},jid)}
  for(const deleteMedia of [true,false])for(const fromMe of [true,false,undefined])run({deleteForMe:{deleteMedia,key:{remoteJid:jid,id:'DEL',fromMe},timestamp:1700000005}},jid)
  for(const pin of [true,false])run({pin},jid)
  run({contact:null},jid);run({contact:{}},jid);run({contact:{fullName:'Alice ☕',firstName:'Alice',lidJid:'300@lid',saveOnPrimaryAddressbook:true}},jid)
  run({disableLinkPreviews:{isPreviewsDisabled:true}},jid);run({disableLinkPreviews:{isPreviewsDisabled:false}},jid);run({disableLinkPreviews:{}},jid)
  for(const star of [true,false])for(const fromMe of [true,false,undefined])run({star:{messages:[{id:'S1',fromMe},{id:'S2',fromMe:true}],star}},jid)
  run({pushNameSetting:'Name ☕'},jid);run({pushNameSetting:''},jid)
  for(const quickReply of [{shortcut:'hi',message:'Hello ☕',timestamp:'1699990000'},{shortcut:'hi',message:'Hello'},{timestamp:'1699990000',deleted:true},{}])run({quickReply},jid)
  for(const addLabel of [{id:'7',name:'Label ☕',color:3,predefinedId:2,deleted:false},{id:'8',name:'Gone',deleted:true},{id:'9'}])run({addLabel},jid)
  run({addChatLabel:{labelId:'7'}},jid);run({removeChatLabel:{labelId:'7'}},jid);run({addMessageLabel:{messageId:'M1',labelId:'7'}},jid);run({removeMessageLabel:{messageId:'M1',labelId:'7'}},jid)
 }
 run({},'200@s.whatsapp.net');run({unknown:true},'200@s.whatsapp.net')
 await writeFile('tests/fixtures/chat_modifications.json',JSON.stringify({now:NOW,cases},null,1)+'\n');console.log(`Generated ${cases.length} original chat modification cases`)
}finally{await rm(temp,{recursive:true,force:true})}
