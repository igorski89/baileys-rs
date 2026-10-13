// Execute the original MIT encrypt, privacy_token and bad-ack handlers with the socket calls stubbed.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-handlers-'))
const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):v
// A fixed clock; the replay shifts every timestamp by the distance to its own.
const now=1800000000,fresh=now,expired=now-60*86400
Date.now=()=>now*1000
try{
 for(const name of ['constants','jid-utils','generic-utils']){
  let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
  s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}")
  s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
  await writeFile(join(temp,`${name}.mjs`),s)
 }
 await writeFile(join(temp,'binary.mjs'),"export * from './jid-utils.mjs';export * from './generic-utils.mjs';")
 await writeFile(join(temp,'tokens.mjs'),stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/tc-token-utils.ts'),'utf8')).replace("'../WABinary'","'./binary.mjs'"))
 const identity=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/identity-change-handler.ts'),'utf8')).replace("import NodeCache from '@cacheable/node-cache'",'').replace("'../WABinary'","'./binary.mjs'").replace("import { isStringNullOrEmpty } from './generics'","const isStringNullOrEmpty = value => value == null || value === ''")
 await writeFile(join(temp,'identity.mjs'),identity)
 const recv=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),defaults=await readFile(join(upstream,'src/Defaults/index.ts'),'utf8')
 const cut=(from,to)=>{const a=recv.indexOf(from),b=recv.indexOf(to,a);if(a<0||b<0)throw Error('upstream slice not found: '+from);return recv.slice(a,b)}
 const reissue=cut('const reissueTcTokenAfterIdentityChange =','const handleEncryptNotification =')
 const encrypt=cut('const handleEncryptNotification =','const handleGroupNotification =')
 const token=cut('const handlePrivacyTokenNotification =','async function decipherLinkPublicKey')
 const ack=cut('const handleBadAck =','\t/// processes a node with the given function')
 // Constants are read from upstream rather than restated here.
 const constant=name=>{const m=defaults.match(new RegExp('export const '+name+' = ([^\\n]+)'));if(!m)throw Error('constant not found: '+name);return m[1]}
 const decode=await readFile(join(upstream,'src/Utils/decode-wa-message.ts'),'utf8')
 const block=name=>{const m=decode.match(new RegExp('export const '+name+' = \\{[^}]+\\}'));if(!m)throw Error('definition not found: '+name);return m[0].replace(/^export /,'')}
 const reasons=block('NACK_REASONS'),codes=block('SERVER_ERROR_CODES')
 const restricted=decode.match(/export const ACCOUNT_RESTRICTED_TEXT = ([^\n]+)/)
 if(!restricted)throw Error('restricted text not found')
 const imports=`import {getBinaryNodeChild,isLidUser,jidNormalizedUser,S_WHATSAPP_NET} from './binary.mjs';import {isTcTokenExpired,resolveIssuanceJid,resolveTcTokenJid,storeTcTokensFromIqResult} from './tokens.mjs';import {handleIdentityChange} from './identity.mjs';const MIN_PREKEY_COUNT = ${constant('MIN_PREKEY_COUNT')};${reasons};${codes};const ACCOUNT_RESTRICTED_TEXT = ${restricted[1]};const WAMessageStatus={ERROR:0};`
 const factory=`export function make(seed){const log=[],events=[],store={tctoken:structuredClone(seed.tokens??{})};
 const keys={get:async(type,ids)=>Object.fromEntries(ids.filter(id=>store[type]?.[id]).map(id=>[id,store[type][id]])),set:async data=>{for(const type in data)for(const id in data[type]){store[type]??={};if(data[type][id]===null)delete store[type][id];else store[type][id]=data[type][id]}}};
 const authState={creds:{me:seed.me},keys},logger={debug(){},error(){},info(){},warn(){},trace(){}};
 const lidMapping={getLIDForPN:async pn=>seed.lids?.[pn]??null,getPNForLID:async lid=>Object.entries(seed.lids??{}).find(([,v])=>v===lid)?.[0]??null};
 const signalRepository={lidMapping,validateSession:async jid=>({exists:(seed.sessions??[]).includes(jid)})};
 const getLIDForPN=lidMapping.getLIDForPN.bind(lidMapping),sock={serverProps:{lidTrustedTokenIssueToLid:!!seed.issueToLid}};
 const assertSessions=async(jids,force)=>{log.push({call:'assertSessions',jids,force});if(seed.assertFails)throw Error('no bundle');return true};
 const uploadPreKeys=async()=>{log.push({call:'uploadPreKeys'})};
 const issuePrivacyTokens=async(jids,timestamp)=>{log.push({call:'issuePrivacyTokens',jids,timestamp});if(seed.issueFails)throw Error('rejected');return {tag:'iq',attrs:{type:'result'},content:[{tag:'tokens',attrs:{},content:jids.map(jid=>({tag:'token',attrs:{jid,t:String(${fresh}),type:'trusted_contact'},content:Buffer.from('issued for '+jid)}))}]}};
 const fetchAccountReachoutTimelock=async()=>{log.push({call:'fetchAccountReachoutTimelock'})};
 const trackTcTokenJid=jid=>{log.push({call:'track',jid})};
 const ev={emit(name,data){events.push({name,data:structuredClone(data)})}},unixTimestampSeconds=()=>${now};
 const debounced=new Set(),identityAssertDebounce={get:key=>debounced.has(key),set:key=>{debounced.add(key)}};
 const inFlightPreKeyLow=new Set(),inFlight463Recoveries=new Set();
 `
 await writeFile(join(temp,'handlers.mjs'),imports+stripTypeScriptTypes(factory+reissue+encrypt+token+ack+'return {handleEncryptNotification,handlePrivacyTokenNotification,handleBadAck,log,events,store}}'))
 const {make}=await import(pathToFileURL(join(temp,'handlers.mjs')))
 const node=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})})
 const me={id:'100:7@s.whatsapp.net',lid:'900:7@lid'},lids={'200@s.whatsapp.net':'2000@lid'}
 // Fire-and-forget work settles within a few turns of the loop.
 const settle=()=>new Promise(r=>setTimeout(r,5))
 async function run(method,input,seed){
  const h=make({me,...seed});let error=false
  try{await h[method](input)}catch{error=true}
  await settle()
  return {node:clean(input),seed:clean(seed),error,log:h.log,events:clean(h.events),tokens:clean(h.store.tctoken)}
 }
 const encrypt_=(attrs,children)=>node('notification',{id:'E1',type:'encrypt',t:'1700000000',...attrs},children)
 const prekeys=[]
 for(const count of [node('count',{value:'0'}),node('count',{value:'4'}),node('count',{value:'5'}),node('count',{value:'100'}),node('count',{value:'4.9'}),node('count',{value:''}),node('count',{value:'many'}),node('count',{}),node('count',{value:'-1'}),node('count',{value:' 3 '}),node('count',{value:'0x2'}),node('count',{value:'1e1'})])
  prekeys.push(await run('handleEncryptNotification',encrypt_({from:'@s.whatsapp.net'},[count]),{}))
 prekeys.push(await run('handleEncryptNotification',encrypt_({from:'@s.whatsapp.net'},[]),{}))
 prekeys.push(await run('handleEncryptNotification',encrypt_({from:'@s.whatsapp.net',id:undefined},[node('count',{value:'1'})]),{}))
 const identityNode=[node('identity',{})],identities=[]
 const held={token:Buffer.from('held'),timestamp:String(fresh)}
 for(const [attrs,children,seed] of [
  [{},identityNode,{}],
  [{from:''},identityNode,{}],
  [{from:'200@s.whatsapp.net'},[],{sessions:['200@s.whatsapp.net']}],
  [{from:'200@s.whatsapp.net'},[node('other',{})],{sessions:['200@s.whatsapp.net']}],
  [{from:'200:4@s.whatsapp.net'},identityNode,{sessions:['200:4@s.whatsapp.net']}],
  [{from:'200:0@s.whatsapp.net'},identityNode,{sessions:['200:0@s.whatsapp.net']}],
  [{from:'100@s.whatsapp.net'},identityNode,{sessions:['100@s.whatsapp.net']}],
  [{from:'900@lid'},identityNode,{sessions:['900@lid']}],
  [{from:'300@s.whatsapp.net'},identityNode,{}],
  [{from:'200@s.whatsapp.net',offline:'1'},identityNode,{sessions:['200@s.whatsapp.net']}],
  [{from:'200@s.whatsapp.net',offline:''},identityNode,{sessions:['200@s.whatsapp.net']}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net']}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],assertFails:true}],
  [{from:'2000@lid'},identityNode,{sessions:['2000@lid'],lids}],
  // Token re-issuance needs an unexpired sender timestamp under the storage JID.
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],tokens:{'200@s.whatsapp.net':{...held,senderTimestamp:fresh}}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],tokens:{'200@s.whatsapp.net':{...held,senderTimestamp:expired}}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],tokens:{'200@s.whatsapp.net':held}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],lids,tokens:{'2000@lid':{...held,senderTimestamp:fresh}}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],lids,issueToLid:true,tokens:{'2000@lid':{...held,senderTimestamp:fresh}}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],lids,tokens:{'200@s.whatsapp.net':{...held,senderTimestamp:fresh}}}],
  [{from:'2000@lid'},identityNode,{sessions:['2000@lid'],lids,tokens:{'2000@lid':{...held,senderTimestamp:fresh}}}],
  [{from:'2000@lid'},identityNode,{sessions:['2000@lid'],lids,issueToLid:true,tokens:{'2000@lid':{...held,senderTimestamp:fresh}}}],
  [{from:'3000@lid'},identityNode,{sessions:['3000@lid'],tokens:{'3000@lid':{...held,senderTimestamp:fresh}}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],issueFails:true,tokens:{'200@s.whatsapp.net':{...held,senderTimestamp:fresh}}}],
  [{from:'200@s.whatsapp.net'},identityNode,{sessions:['200@s.whatsapp.net'],assertFails:true,tokens:{'200@s.whatsapp.net':{...held,senderTimestamp:fresh}}}],
  [{from:'200@s.whatsapp.net',offline:'1'},identityNode,{sessions:['200@s.whatsapp.net'],tokens:{'200@s.whatsapp.net':{...held,senderTimestamp:fresh}}}]
 ])identities.push(await run('handleEncryptNotification',encrypt_(attrs,children),seed))
 // A repeated notification within the debounce window.
 {const h=make({me,sessions:['200@s.whatsapp.net']}),n=encrypt_({from:'200@s.whatsapp.net'},identityNode);await h.handleEncryptNotification(n);await h.handleEncryptNotification(n);await settle();identities.push({node:clean(n),seed:{sessions:['200@s.whatsapp.net']},repeat:true,error:false,log:h.log,events:[],tokens:{}})}
 const tok=(attrs,content=Buffer.from('incoming'))=>node('token',{type:'trusted_contact',t:String(fresh),...attrs},content)
 const privacy=(attrs,children)=>node('notification',{id:'P1',type:'privacy_token',t:'1700000000',from:'200@s.whatsapp.net',...attrs},children)
 const tokensNode=(...list)=>[node('tokens',{},list)],tokens=[]
 for(const [attrs,children,seed] of [
  [{},tokensNode(tok({})),{}],
  [{},tokensNode(tok({})),{lids}],
  [{sender_lid:'2000:3@lid'},tokensNode(tok({})),{}],
  [{sender_lid:'2000@lid'},tokensNode(tok({})),{lids:{'200@s.whatsapp.net':'2999@lid'}}],
  [{sender_lid:'200@s.whatsapp.net'},tokensNode(tok({})),{lids}],
  [{sender_lid:''},tokensNode(tok({})),{lids}],
  [{from:'200:5@s.whatsapp.net'},tokensNode(tok({})),{lids}],
  [{from:'2000@lid'},tokensNode(tok({})),{}],
  [{from:'2000:9@lid'},tokensNode(tok({})),{}],
  [{from:'0@s.whatsapp.net'},tokensNode(tok({})),{}],
  [{from:'13135551234@s.whatsapp.net'},tokensNode(tok({})),{}],
  [{from:'13165550012@s.whatsapp.net'},tokensNode(tok({})),{}],
  [{from:'123@bot'},tokensNode(tok({})),{}],
  [{from:'group@g.us'},tokensNode(tok({})),{}],
  [{from:'200@c.us'},tokensNode(tok({})),{}],
  [{},[],{}],
  [{},[node('tokens',{})],{}],
  [{},tokensNode(tok({type:'other'})),{}],
  [{},tokensNode(tok({type:undefined})),{}],
  [{},tokensNode(tok({},undefined)),{}],
  [{},tokensNode(tok({},Buffer.alloc(0))),{}],
  [{},tokensNode(tok({t:undefined})),{}],
  [{},tokensNode(tok({t:'0'})),{}],
  [{},tokensNode(tok({t:''})),{}],
  [{},tokensNode(tok({t:'soon'})),{}],
  [{},tokensNode(tok({t:String(expired)})),{}],
  // The token's own jid is the receiving device and is ignored.
  [{},tokensNode(tok({jid:'100:7@s.whatsapp.net'})),{}],
  [{},tokensNode(tok({t:String(fresh-10)}),tok({t:String(fresh)},Buffer.from('second')),tok({type:'other'},Buffer.from('third'))),{}],
  [{},tokensNode(tok({t:String(fresh)}),tok({t:String(fresh-10)},Buffer.from('older'))),{}],
  [{},tokensNode(tok({t:String(fresh-10)})),{tokens:{'200@s.whatsapp.net':{token:Buffer.from('held'),timestamp:String(fresh),senderTimestamp:fresh}}}],
  [{},tokensNode(tok({t:String(fresh)})),{tokens:{'200@s.whatsapp.net':{token:Buffer.from('held'),timestamp:String(fresh),senderTimestamp:fresh}}}],
  [{},tokensNode(tok({t:String(fresh)})),{tokens:{'200@s.whatsapp.net':{token:Buffer.from('held'),timestamp:String(fresh-10),senderTimestamp:fresh-20}}}],
  [{},tokensNode(tok({t:String(fresh)})),{tokens:{'200@s.whatsapp.net':{token:Buffer.alloc(0),senderTimestamp:fresh}}}],
  [{},tokensNode(tok({t:String(fresh)})),{lids,tokens:{'200@s.whatsapp.net':{token:Buffer.from('held'),timestamp:String(fresh+5)}}}],
  [{},tokensNode(tok({t:String(fresh)})),{lids,tokens:{'2000@lid':{token:Buffer.from('held'),timestamp:String(fresh+5)}}}]
 ])tokens.push(await run('handlePrivacyTokenNotification',privacy(attrs,children),seed))
 const ack_=attrs=>node('ack',{class:'message',id:'A1',from:'200@s.whatsapp.net',...attrs}),acks=[]
 for(const [attrs,seed] of [
  [{},{}],
  [{error:''},{}],
  [{error:'500'},{}],
  [{error:'479'},{}],
  [{error:'475'},{}],
  [{error:'0463'},{}],
  [{error:'463'},{}],
  [{error:'463',id:undefined},{}],
  [{error:'463',from:undefined},{}],
  [{error:'463',from:''},{}],
  [{error:'463'},{lids}],
  [{error:'463'},{lids,issueToLid:true}],
  [{error:'463',from:'2000@lid'},{lids}],
  [{error:'463',from:'2000@lid'},{lids,issueToLid:true}],
  [{error:'463',from:'3000@lid'},{}],
  [{error:'463'},{issueFails:true}],
  [{error:'463'},{tokens:{'200@s.whatsapp.net':{token:Buffer.from('held'),timestamp:String(fresh-10),senderTimestamp:fresh-20}}}],
  [{error:'463',from:'group@g.us',participant:'200@s.whatsapp.net'},{}],
  [{error:'500',from:'group@g.us',participant:'200@s.whatsapp.net',phash:'2:abc'},{}]
 ])acks.push(await run('handleBadAck',ack_(attrs),seed))
 await writeFile('tests/fixtures/notification-handlers.json',JSON.stringify({now,me,prekeys,identities,tokens,acks},null,1)+'\n')
 console.log(`Generated ${prekeys.length} pre-key, ${identities.length} identity, ${tokens.length} token and ${acks.length} ack cases`)
}finally{await rm(temp,{recursive:true,force:true})}
