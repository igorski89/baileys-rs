// Original history extraction and Node zlib behavior, with synthetic payloads.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {deflateSync,inflateSync,gzipSync,deflateRawSync} from 'node:zlib'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-history-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const history=await readFile(join(upstream,'src/Utils/history.ts'),'utf8'),jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8');
 const classifiers=jid.split('\n').filter(line=>/^export const (isPnUser|isLidUser|isHostedPnUser|isHostedLidUser) /.test(line)).join('\n');
 const extract=history.slice(history.indexOf('const extractPnFromMessages'),history.indexOf('export const downloadHistory'))+history.slice(history.indexOf('export const processHistoryMessage'),history.indexOf('export const downloadAndProcessHistorySyncNotification'));
 await writeFile(join(temp,'helpers.mjs'),`import {proto} from './proto.mjs';const WAMessageStubType=proto.WebMessageInfo.StubType,toNumber=t=>typeof t==='object'&&t?('toNumber'in t?t.toNumber():t.low):t||0;\n`+stripTypeScriptTypes(classifiers+'\n'+extract));
 const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),wire=(t,v)=>Buffer.from(t.encode(v).finish()).toString('hex');
 const message=(fromMe,timestamp,receipts=[],extra={})=>({message:{key:{remoteJid:'201@lid',id:'m'+timestamp,fromMe},messageTimestamp:timestamp,userReceipt:receipts.map(userJid=>({userJid})),...extra},msgOrderId:9});
 const conversations=[
  {id:'200@s.whatsapp.net',displayName:'Display',name:'Name',username:'user',lidJid:'201@lid',messages:[message(true,90,['200@s.whatsapp.net']),message(false,80),message(false,70)]},
  {id:'201@lid',pnJid:'200@s.whatsapp.net',accountLid:'other@lid',messages:[message(false,0),message(false,80)]},
  {id:'201@lid',messages:[message(false,90,['wrong@s.whatsapp.net']),message(true,80,['201@lid','200@s.whatsapp.net']),message(true,70,['200@hosted']),message(true,60,['300@s.whatsapp.net'])]},
  {id:'201@hosted.lid',name:'Name',messages:[message(true,90,['200@hosted'])]},
  {id:'200@hosted',lidJid:'201@hosted.lid',messages:[]},
  {id:'200@g.us',accountLid:'ignored@lid',messages:[message(false,20,[],{key:{remoteJid:'200@g.us',participant:'300@s.whatsapp.net',fromMe:false},messageStubType:proto.WebMessageInfo.StubType.BIZ_PRIVACY_MODE_TO_BSP,messageStubParameters:['Verified']}),message(true,10,[],{messageStubType:proto.WebMessageInfo.StubType.BIZ_PRIVACY_MODE_TO_FB,messageStubParameters:['Other']})]},
  {id:'201@lid',displayName:'',name:'',username:'fallback',pnJid:'',lidJid:'',accountLid:'202@lid',messages:[]},
  {id:'201@lid',messages:[{}]},
  {id:'201@lid',messages:[{message:{messageTimestamp:10}}]},
  {id:'201@lid',messages:[message(false,'9007199254740993')]},
 ];
 const cases=[];
 for(const syncType of [0,1,2,3,4,5,6,99])for(let variant=0;variant<conversations.length;variant++) {
  const item=proto.HistorySync.decode(proto.HistorySync.encode(proto.HistorySync.fromObject({syncType,progress:variant===0?0:50,conversations:[conversations[variant]],pushnames:[{id:'200@s.whatsapp.net',pushname:'Notify'},{id:'201@lid',pushname:''}],phoneNumberToLidMappings:[{pnJid:'100@s.whatsapp.net',lidJid:'101@lid'},{pnJid:'',lidJid:'ignored@lid'},{}],pastParticipants:[{groupJid:'200@g.us'}]})).finish());
  const input=wire(proto.HistorySync,item);let result,error;try{result=h.processHistoryMessage(item)}catch(e){error=e.message}
  cases.push({syncType,variant,input,mutated:wire(proto.HistorySync,item),error,expected:result?{chats:result.chats.map(c=>({wire:wire(proto.Conversation,c),received:c.lastMessageRecvTimestamp??null})),contacts:JSON.parse(JSON.stringify(result.contacts)),messages:result.messages.map(m=>wire(proto.WebMessageInfo,m)),mappings:result.lidPnMappings,past:result.pastParticipants.map(p=>wire(proto.PastParticipants,p)),syncType:result.syncType,progress:result.progress}:null});
 }
 const inflation=[];
 for(const size of [0,1,15,16,65536,200003]){
  const plain=Buffer.from(Array.from({length:size},(_,i)=>i%251)),good=deflateSync(plain);
  for(const [variant,input] of [['good',good],['tail',Buffer.concat([good,Buffer.from('ignored')])],['truncated',good.subarray(0,good.length-1)],['raw',deflateRawSync(plain)],['gzip',gzipSync(plain)],['empty',Buffer.alloc(0)]]){let output,error;try{output=inflateSync(input)}catch(e){error=e.code}inflation.push({size,variant,input:input.toString('hex'),output:output?.toString('hex'),error})}
 }
 const jidModule=stripTypeScriptTypes(jid.replace(/export enum WAJIDDomains \{[^}]*\}/,'export const WAJIDDomains = {WHATSAPP:0,LID:1,HOSTED:128,HOSTED_LID:129}'));
 await writeFile(join(temp,'binary.mjs'),jidModule+'\nexport const getBinaryNodeChild=(n,t)=>n?.content?.find(c=>c.tag===t),getBinaryNodeChildren=(n,t)=>n?.content?.filter(c=>c.tag===t)||[];');
 const tc=await readFile(join(upstream,'src/Utils/tc-token-utils.ts'),'utf8');await writeFile(join(temp,'tc.mjs'),stripTypeScriptTypes(tc).replace("'../WABinary'","'./binary.mjs'"));
 const processSource=await readFile(join(upstream,'src/Utils/process-message.ts'),'utf8');const storeFunction=processSource.slice(processSource.indexOf('async function storeTcTokensFromHistorySync'),processSource.indexOf('/** Cleans a received message'));
 await writeFile(join(temp,'tokens.mjs'),`import {resolveTcTokenJid,buildMergedTcTokenIndexWrite} from './tc.mjs';import {jidNormalizedUser} from './binary.mjs';const toNumber=t=>typeof t==='object'&&t?('toNumber'in t?t.toNumber():t.low):t||0;export `+stripTypeScriptTypes(storeFunction));const tokens=await import(pathToFileURL(join(temp,'tokens.mjs')));
 const tokenCases=[];
 for(const timestamp of [null,'bad','0','100','300'])for(const variant of ['new','equal','older','duplicate','empty','zero','senderZero']) {
  const rows=[{id:'200:2@s.whatsapp.net',tcToken:Buffer.from([1,2]),tcTokenTimestamp:200}, {id:'400@s.whatsapp.net',tcToken:Buffer.from([3]),tcTokenTimestamp:201}];
  if(variant==='equal')rows[0].tcTokenTimestamp=100;if(variant==='older')rows[0].tcTokenTimestamp=10;if(variant==='empty')rows[0].tcToken=Buffer.alloc(0);if(variant==='zero')rows[0].tcTokenTimestamp=0;if(variant==='senderZero')rows[0].tcTokenSenderTimestamp=0;
  if(variant==='duplicate')rows.push({id:'201@lid',tcToken:Buffer.from([9]),tcTokenTimestamp:150});
  const chats=rows.map(r=>proto.Conversation.decode(proto.Conversation.encode(proto.Conversation.fromObject(r)).finish()));
  const state={'201@lid':{token:Buffer.from([7]),senderTimestamp:777,kept:'yes',...(timestamp===null?{}:{timestamp})},__index:{token:Buffer.from(JSON.stringify(['old@s.whatsapp.net']))}};
  const convert=state=>Object.fromEntries(Object.entries(state).map(([k,v])=>[k,{...v,token:Buffer.from(v.token).toString('hex')}]));const initial=convert(state),calls=[];
  const keyStore={get:async(type,ids)=>Object.fromEntries(ids.filter(id=>state[id]).map(id=>[id,state[id]])),set:async data=>{calls.push(Object.keys(data.tctoken));Object.assign(state,data.tctoken)}};
  await tokens.storeTcTokensFromHistorySync(chats,{lidMapping:{getLIDForPN:async pn=>pn==='200@s.whatsapp.net'?'201@lid':null}},keyStore);
  tokenCases.push({variant,timestamp,chats:chats.map(c=>wire(proto.Conversation,c)),initial,expected:convert(state),writes:calls});
 }
 const branch=processSource.slice(processSource.indexOf('case proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION:'),processSource.indexOf('case proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE:'));
 const guard=processSource.slice(processSource.indexOf('const SELF_ONLY_TYPES ='),processSource.indexOf('\n\t\tswitch (protocolMsg.type)'));
 await writeFile(join(temp,'branch.mjs'),`import {proto} from './proto.mjs';import {processHistoryMessage} from './helpers.mjs';import {storeTcTokensFromHistorySync} from './tokens.mjs';export async function run(message,old,enabled,fail,payload){const events=[],keys={},maps=[],logger={info(){},warn(){},debug(){}},creds={processedHistoryMessages:old?[{key:{id:'OLD'},messageTimestamp:9}]:[]},ev={emit:(name,data)=>events.push({name,data})},protocolMsg=message.message.protocolMessage,shouldProcessHistoryMsg=enabled,options={},keyStore={get:async(type,ids)=>Object.fromEntries(ids.filter(id=>keys[id]).map(id=>[id,keys[id]])),set:async data=>Object.assign(keys,data.tctoken)},signalRepository={lidMapping:{storeLIDPNMappings:async pairs=>maps.push(...pairs),getLIDForPN:async pn=>maps.find(m=>m.pn===pn)?.lid??null}},downloadAndProcessHistorySyncNotification=async()=>{if(fail)throw Error('download failed');return processHistoryMessage(proto.HistorySync.decode(payload))};let error;try{`+stripTypeScriptTypes('async function execute(){'+guard+'switch(protocolMsg.type){'+branch+'}}')+`await execute()}catch(e){error=e.message}return{events,maps,keys,error};}`);
 const b=await import(pathToFileURL(join(temp,'branch.mjs'))),payload=proto.HistorySync.encode(proto.HistorySync.fromObject({syncType:3,progress:50,conversations:[{id:'200@s.whatsapp.net',lidJid:'201@lid',tcToken:Buffer.from([5]),tcTokenTimestamp:100,tcTokenSenderTimestamp:0}],phoneNumberToLidMappings:[{pnJid:'100@s.whatsapp.net',lidJid:'101@lid'}]})).finish(),protocolCases=[];
 for(const syncType of [null,0,2,3,6,99])for(const enabled of [false,true])for(const fromMe of [false,true])for(const old of [false,true])for(const fail of [false,true]) {
  const notification={...(syncType===null?{}:{syncType}),chunkOrder:0,peerDataRequestSessionId:''},message=proto.WebMessageInfo.decode(proto.WebMessageInfo.encode(proto.WebMessageInfo.fromObject({key:{id:'HISTORY',remoteJid:'100@s.whatsapp.net',fromMe},messageTimestamp:1700000000,message:{protocolMessage:{type:proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,historySyncNotification:notification}}})).finish()),eligible=enabled&&syncType!==null&&syncType>=0&&syncType<=6;
  const result=await b.run(message,old,eligible,fail,payload);
  protocolCases.push({syncType,enabled,fromMe,old,fail,message:wire(proto.WebMessageInfo,message),payload:Buffer.from(payload).toString('hex'),expected:{error:result.error,maps:result.maps,keys:Object.fromEntries(Object.entries(result.keys).map(([id,k])=>[id,{...k,token:Buffer.from(k.token).toString('hex')}])),events:result.events.map(e=>e.name==='creds.update'?{name:e.name,records:e.data.processedHistoryMessages.map(m=>({key:wire(proto.MessageKey,m.key),timestamp:String(m.messageTimestamp)}))}:{name:e.name,isLatest:e.data.isLatest,chunkOrder:e.data.chunkOrder,session:e.data.peerDataRequestSessionId,syncType:e.data.syncType})}});
 }
 await writeFile('tests/fixtures/history.json',JSON.stringify({cases,inflation,tokenCases,protocolCases},null,2)+'\n');console.log(`Generated ${cases.length} history, ${inflation.length} zlib, ${tokenCases.length} token and ${protocolCases.length} protocol cases`)
}finally{await rm(temp,{recursive:true,force:true})}
