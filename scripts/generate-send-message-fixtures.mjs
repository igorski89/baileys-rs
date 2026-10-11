// Run the pinned MIT message construction helpers against synthetic protobufs.
import {stripTypeScriptTypes, createRequire} from 'node:module'
import {readFile, writeFile, mkdtemp, rm} from 'node:fs/promises'
import {resolve, join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream = resolve(process.argv[2] ?? '../Baileys')
const require = createRequire(resolve('.cache/fixture-tools/package.json'))
const temp = await mkdtemp(join(tmpdir(), 'baileys-send-'))
try {
  const source = (await readFile(join(upstream, 'WAProto/index.js'), 'utf8')).replace('"protobufjs/minimal.js"', JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp, 'proto.mjs'), source)
  for (const name of ['constants', 'jid-utils']) {
    let text = await readFile(join(upstream, `src/WABinary/${name}.ts`), 'utf8')
    text = text.replace(/export enum WAJIDDomains \{([^}]+)\}/, (_, body) => 'export const WAJIDDomains = {' + body.replaceAll('=', ':') + '}')
    await writeFile(join(temp, `${name}.mjs`), stripTypeScriptTypes(text).replaceAll("'./constants'", "'./constants.mjs'"))
  }
  const messages = await readFile(join(upstream, 'src/Utils/messages.ts'), 'utf8')
  const sendSource=await readFile(join(upstream,'src/Socket/messages-send.ts'),'utf8'),chatsSource=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8'),processing=await readFile(join(upstream,'src/Utils/process-message.ts'),'utf8')
  const send=sendSource.slice(sendSource.indexOf('sendMessage: async'),sendSource.lastIndexOf('\n\t}\n}')).replace('sendMessage:', 'const sendMessage =')
  const upsert=chatsSource.slice(chatsSource.indexOf('const upsertMessage ='),chatsSource.indexOf("ws.on('CB:presence'"))
  const real=processing.slice(processing.indexOf('const REAL_MSG_STUB_TYPES'),processing.indexOf('async function storeTcTokensFromHistorySync'))
  const classifiers=processing.slice(processing.indexOf('export const isRealMessage'),processing.indexOf('type PollContext'))
  const processMessage=processing.slice(processing.indexOf('const processMessage =')).replace('export default processMessage','')
  const helpers=messages.slice(messages.indexOf('export const generateWAMessageFromContent'),messages.indexOf('export const generateWAMessage ='))+messages.slice(messages.indexOf('export const getContentType'),messages.indexOf('export const extractMessageContent'))
  const prefix=`import {proto} from './proto.mjs';import {isJidGroup,isJidNewsletter,isJidStatusBroadcast,isJidBroadcast,jidNormalizedUser} from './jid-utils.mjs';const WAProto=proto,WAMessageStatus=proto.WebMessageInfo.Status,WAMessageStubType=proto.WebMessageInfo.StubType,WA_DEFAULT_EPHEMERAL=604800;const generateMessageIDV2=()=> 'DEFAULT',unixTimestampSeconds=d=>Math.floor(d.getTime()/1000),toNumber=v=>typeof v==='object'&&v?v.toNumber():v;class Boom extends Error{};`
  const factory=`export function make(payload,emitOwnEvents,unarchiveChats){const events=[],relays=[],toggles=[],pending=[];const authState={creds:{me:{id:'100:7@s.whatsapp.net'},accountSettings:{unarchiveChats}}};const creds=authState.creds,config={emitOwnEvents,options:{}},logger={info(){},debug(){},warn(){},error(){}},messageMutex={mutex:fn=>fn()},process={nextTick:fn=>pending.push(fn)},ev={emit:(name,data)=>events.push({name,data}),createBufferedFunction:fn=>fn,flush(){}};const SyncState={Online:1,Syncing:2,AwaitingInitialSync:3},syncState=SyncState.Online;const getHistoryMsg=()=>undefined,getMessage=async()=>undefined;const sock={user:creds.me};const getUrlInfo=()=>undefined,linkPreviewImageThumbnailWidth=192,httpRequestOptions={},generateHighQualityLinkPreview=false,waUploadToServer=()=>{},signalRepository={},keyStore={},placeholderResendCache={};const groupToggleEphemeral=async(jid,value)=>toggles.push({jid,value});const relayMessage=async(jid,message,options)=>relays.push({jid,message,options});const generateWAMessage=async(jid,content,options)=>generateWAMessageFromContent(jid,proto.Message.decode(proto.Message.encode(payload).finish()),options);`
  await writeFile(join(temp,'helpers.mjs'),prefix+stripTypeScriptTypes(real+classifiers+helpers+processMessage)+stripTypeScriptTypes(factory+upsert+send+`;return {sendMessage,events,relays,toggles,drain:async()=>{for(const fn of pending)await fn()}}}`))
  const {proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),{make}=await import(pathToFileURL(join(temp,'helpers.mjs')))
  const hex=b=>Buffer.from(b).toString('hex'),wire=m=>hex(proto.Message.encode(m).finish()),kw=k=>k===undefined?null:hex(proto.MessageKey.encode(proto.MessageKey.fromObject(k)).finish())
  function event({name,data}) {
    if(name==='messages.upsert')return {name,messages:data.messages.map(m=>hex(proto.WebMessageInfo.encode(m).finish())),type:data.type}
    if(name==='chats.update')return {name,updates:data.map(c=>({...c,messages:c.messages?.map(m=>hex(proto.WebMessageInfo.encode(m.message).finish()))}))}
    if(name==='messages.update')return {name,updates:data.map(m=>({key:kw(m.key),...(m.update.message===null?{revoke:true,stub:m.update.messageStubType,originalKey:kw(m.update.key)}:{message:wire(m.update.message),timestamp:typeof m.update.messageTimestamp==='object'?m.update.messageTimestamp.toNumber():m.update.messageTimestamp})}))}
    if(name==='messages.reaction')return {name,updates:data.map(m=>({key:kw(m.key),reaction:hex(proto.Reaction.encode(proto.Reaction.fromObject(m.reaction)).finish())}))}
    throw Error('unexpected event '+name)
  }
  const key={remoteJid:'200@s.whatsapp.net',id:'TARGET',fromMe:true}
  const payloads=[
    {kind:'text',input:{text:'hello'},payload:{extendedTextMessage:{text:'hello'}}},
    ...[true,false].map(fromMe=>({kind:'reaction',input:{react:{key:{...key,fromMe},text:'💚'}},payload:{reactionMessage:{key:{...key,fromMe},text:'💚',senderTimestampMs:1700000000123}}})),
    ...[key,{...key,remoteJid:'group@g.us',fromMe:false},{...key,remoteJid:'group@g.us',fromMe:true}].map(k=>({kind:'delete',input:{delete:k},payload:{protocolMessage:{key:k,type:0}}})),
    {kind:'edit',input:{text:'edited',edit:key},payload:{protocolMessage:{key,type:14,editedMessage:{extendedTextMessage:{text:'edited'}},timestampMs:1700000000123}}},
    {kind:'deleteEdit',input:{delete:key,edit:key},payload:{protocolMessage:{key,type:14,editedMessage:{protocolMessage:{key,type:0}},timestampMs:-1555}}},
    {kind:'pin',input:{pin:key,type:1},payload:{pinInChatMessage:{key,type:1,senderTimestampMs:1700000000123}}},
    {kind:'poll',input:{poll:{name:'Poll',values:['A']}},payload:{pollCreationMessage:{name:'Poll',options:[{optionName:'A'}],selectableOptionsCount:0}}},
    {kind:'pollEdit',input:{poll:{name:'Poll',values:['A']},edit:key},payload:{protocolMessage:{key,type:14,editedMessage:{pollCreationMessage:{name:'Poll'}},timestampMs:0}}},
    {kind:'event',input:{event:{name:'Event'}},payload:{eventMessage:{name:'Event'}}},
    ...[true,false,60].map(value=>({kind:'disappearing',input:{disappearingMessagesInChat:value},payload:{ephemeralMessage:{message:{protocolMessage:{type:3,ephemeralExpiration:value===true?604800:value===false?0:value}}}}})),
    {kind:'request',input:{requestPhoneNumber:false},payload:{requestPhoneNumberMessage:{}}},
    {kind:'encReaction',input:{prepared:true},payload:{encReactionMessage:{}}},
    {kind:'wrappedReaction',input:{react:{key},viewOnce:true},payload:{viewOnceMessage:{message:{reactionMessage:{key,text:'👍'}}}}},
  ]
  payloads.push({kind:'editMissingFields',input:{text:'edited',edit:key},payload:{protocolMessage:{type:14}}})
  const cases=[]
  for(const p of payloads)for(const emit of [false,true])for(const unarchive of [false,true]) {
    const recipient=p.kind==='disappearing'?'200@s.whatsapp.net':'group@g.us'
    const payload=proto.Message.fromObject(p.payload),h=make(payload,emit,unarchive)
    const full=await h.sendMessage(recipient,p.input,{timestamp:new Date(1700000000123),messageId:'GENERATED',useCachedGroupMetadata:false,statusJidList:['300@lid']});await h.drain()
    cases.push({kind:p.kind,input:p.input,recipient,emit,unarchive,payload:wire(payload),full:full?hex(proto.WebMessageInfo.encode(full).finish()):null,events:h.events.map(event),relays:h.relays.map(r=>({jid:r.jid,message:wire(r.message),messageId:r.options.messageId,useCachedGroupMetadata:r.options.useCachedGroupMetadata,statusJidList:r.options.statusJidList,attributes:r.options.additionalAttributes,nodes:r.options.additionalNodes})),toggles:h.toggles})
  }
  for(const value of [true,false,60]) {const h=make({},true,true);await h.sendMessage('group@g.us',{disappearingMessagesInChat:value},{timestamp:new Date(1700000000123),messageId:'GENERATED'});await h.drain();cases.push({kind:'groupDisappearing',input:{disappearingMessagesInChat:value},recipient:'group@g.us',emit:true,unarchive:true,events:h.events.map(event),toggles:h.toggles})}
  await writeFile('tests/fixtures/send_message.json',JSON.stringify({cases},null,2)+'\n');console.log(`Generated ${cases.length} send and own-event cases`)
}finally {await rm(temp,{recursive:true,force:true})}
