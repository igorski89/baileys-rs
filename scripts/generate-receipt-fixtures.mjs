// Execute pinned MIT receipt/ACK/privacy helpers, using synthetic nodes only.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-receipts-'))
const hex=b=>Buffer.from(b).toString('hex')
try {
  for(const name of ['constants','jid-utils','encode']){let source=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');source=source.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}');await writeFile(join(temp,`${name}.mjs`),stripTypeScriptTypes(source).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,''))}
  const send=await readFile(join(upstream,'src/Socket/messages-send.ts'),'utf8'),messages=await readFile(join(upstream,'src/Utils/messages.ts'),'utf8'),chats=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8'),generic=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8'),recv=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8')
  const aggregation=messages.slice(messages.indexOf('export const aggregateMessageKeysNotFromMe'),messages.indexOf('type DownloadMediaMessageContext'))
  const receipts=send.slice(send.indexOf('const sendReceipt ='),send.indexOf('/** Device info with wire JID */'))
  const dictionary=generic.slice(generic.indexOf('const indexCache'),generic.indexOf('export const getAllBinaryNodeChildren'))+generic.slice(generic.indexOf('export const reduceBinaryNodeToDictionary'),generic.indexOf('export const reduceBinaryNodeToDictionary')+generic.slice(generic.indexOf('export const reduceBinaryNodeToDictionary')).indexOf('\n}\n')+3)
  const privacy=chats.slice(chats.indexOf('const fetchPrivacySettings ='),chats.indexOf('const updateDefaultDisappearingMode ='))
  const reject=recv.slice(recv.indexOf('const rejectCall ='),recv.indexOf('const sendRetryRequest ='))
  await writeFile(join(temp,'ack.mjs'),stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/stanza-ack.ts'),'utf8')))
  await writeFile(join(temp,'helpers.mjs'),`import {isPnUser,isLidUser} from './jid-utils.mjs';class Boom extends Error{};const S_WHATSAPP_NET='@s.whatsapp.net';const unixTimestampSeconds=()=>1700000000;`+stripTypeScriptTypes(aggregation+dictionary)+stripTypeScriptTypes(`export function make(response){let privacySettings;const sent=[],queries=[],logger={debug(){}};const authState={creds:{me:{id:'100:7@s.whatsapp.net'}}};const sendNode=async node=>sent.push(node);const query=async node=>{queries.push(node);return response};`+privacy+receipts+reject+`return {sent,queries,sendReceipt,sendReceipts,readMessages,fetchPrivacySettings,rejectCall,updateMessagesPrivacy,updateCallPrivacy,updateLastSeenPrivacy,updateOnlinePrivacy,updateProfilePicturePrivacy,updateStatusPrivacy,updateReadReceiptsPrivacy,updateGroupsAddPrivacy}}`))
  const {make,aggregateMessageKeysNotFromMe,reduceBinaryNodeToDictionary}=await import(pathToFileURL(join(temp,'helpers.mjs'))),{buildAckStanza}=await import(pathToFileURL(join(temp,'ack.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
  const receiptCases=[]
  for(const type of [undefined,'read','read-self','hist_sync','peer_msg','sender','inactive','played'])for(const jid of ['200@s.whatsapp.net','800@lid','group@g.us','status@broadcast','42@newsletter','200@c.us','200:99@hosted']){
    const config={jid,participant:'800:1@lid',ids:['ONE','TWO','TWO'],type:type??null},h=make({});await h.sendReceipt(jid,config.participant,config.ids,type);receiptCases.push({config,wire:hex(encodeBinaryNode(h.sent[0]))})
  }
  for(const config of [{jid:'200@lid',participant:null,ids:['ONE'],type:'sender'},{jid:'group@g.us',participant:'',ids:[''],type:null},{jid:'200@lid',participant:null,ids:[null,null],type:null},{jid:'group@g.us',participant:null,ids:[],type:'read'}]){
    const h=make({});let error=false;try{await h.sendReceipt(config.jid,config.participant??undefined,config.ids.map(id=>id??undefined),config.type??undefined)}catch{error=true};receiptCases.push({config,error,wire:error?null:hex(encodeBinaryNode(h.sent[0]))})
  }
  const keys=[{remoteJid:'a@g.us',id:'A',participant:'800@lid'},{remoteJid:'a@g.us',id:'OWN',participant:'800@lid',fromMe:true},{remoteJid:'b@lid',id:'B'},{remoteJid:'a@g.us',id:'A',participant:'800@lid'},{remoteJid:'a@g.us',id:'C',participant:'900@lid'},{remoteJid:'a:g.us',id:'D',participant:'x:y'},{remoteJid:'a:g.us:x',id:'E',participant:'y'},{id:'MISSING'},{remoteJid:'undefined',participant:''},{}]
  const aggregated=aggregateMessageKeysNotFromMe(keys).map(g=>({jid:g.jid??null,participant:g.participant??null,messageIds:g.messageIds.map(id=>id??null)}))
  const ackCases=[]
  for(const tag of ['message','receipt','notification','call','iq'])for(const type of [undefined,'','read','sender','retry','unknown'])for(const error of [undefined,0,500]){
    const node={tag,attrs:{id:'ACK',from:'group@g.us',participant:'800:1@lid',recipient:'200@lid',...(type!==undefined?{type}:{})}},me='100:7@s.whatsapp.net';ackCases.push({node,error:error??null,me,wire:hex(encodeBinaryNode(buildAckStanza(node,error,me)))})
  }
  for(const node of [{tag:'message',attrs:{}},{tag:'message',attrs:{id:'',from:'',participant:'',recipient:'',type:''}},{tag:'message',attrs:{id:'A',from:'200@lid'}},{tag:'ack',attrs:{id:'A',from:'200@lid',extra:'ignored'}}])ackCases.push({node,error:null,me:null,wire:hex(encodeBinaryNode(buildAckStanza(node)))})
  const privacyResponse={tag:'iq',attrs:{},content:[{tag:'privacy',attrs:{},content:[{tag:'category',attrs:{name:'readreceipts',value:'all'}},{tag:'category',attrs:{config_code:'10518',config_value:'1'}},{tag:'category',attrs:{name:'empty',value:'',config_value:'fallback'}},{tag:'category',attrs:{name:'missing'}},{tag:'ignored',attrs:{name:'skip',value:'all'}},{tag:'category',attrs:{name:'duplicate',value:'old'}},{tag:'category',attrs:{name:'duplicate',value:'new'}}]}]}
  const dict=reduceBinaryNodeToDictionary(privacyResponse.content[0],'category'),settings=Object.fromEntries(Object.entries(dict).map(([k,v])=>[k,v??null]))
  const h=make(privacyResponse);await h.fetchPrivacySettings();await h.fetchPrivacySettings();await h.fetchPrivacySettings(true)
  const privacyQueries=h.queries.map(n=>hex(encodeBinaryNode(n)))
  const updates=[]
  for(const [method,values] of [['updateMessagesPrivacy',['all','contacts']],['updateCallPrivacy',['all','known']],['updateLastSeenPrivacy',['all','contacts','contact_blacklist','none']],['updateOnlinePrivacy',['all','match_last_seen']],['updateProfilePicturePrivacy',['none']],['updateStatusPrivacy',['contacts']],['updateReadReceiptsPrivacy',['all','none']],['updateGroupsAddPrivacy',['all','contacts','contact_blacklist']]])for(const value of values){const h=make({});await h[method](value);updates.push({method,value,wire:hex(encodeBinaryNode(h.queries[0]))})}
  const readCases=[];for(const value of ['all','none','other',null]){const response={tag:'iq',attrs:{},content:[{tag:'privacy',attrs:{},content:value?[{tag:'category',attrs:{name:'readreceipts',value}}]:[]}]};const h=make(response);await h.readMessages(keys.slice(0,5));readCases.push({value,wires:h.sent.map(n=>hex(encodeBinaryNode(n)))})}
  const rejection=make({});await rejection.rejectCall('CALL','800@lid')
  await writeFile('tests/fixtures/receipts.json',JSON.stringify({timestamp:1700000000,receiptCases,keys,aggregated,ackCases,privacyResponse,settings,privacyQueries,updates,readCases,rejectCall:hex(encodeBinaryNode(rejection.queries[0]))},null,2)+'\n')
  console.log(`Generated ${receiptCases.length} receipts, ${ackCases.length} ACKs and ${updates.length} privacy updates from upstream`)
}finally{await rm(temp,{recursive:true,force:true})}
