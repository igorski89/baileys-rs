// Execute the pinned upstream group parser and socket with synthetic IQ responses.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-groups-')),hex=b=>Buffer.from(b).toString('hex')
const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):v
try {
  for(const name of ['constants','jid-utils','encode','generic-utils']){
    let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
    s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}")
    s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
    await writeFile(join(temp,`${name}.mjs`),s)
  }
  const source=await readFile(join(upstream,'src/Socket/groups.ts'),'utf8')
  const header=`import {getBinaryNodeChild,getBinaryNodeChildren,getBinaryNodeChildString} from './generic-utils.mjs';import {isLidUser,isPnUser,jidEncode,jidNormalizedUser} from './jid-utils.mjs';const makeChatsSocket=c=>c.sock;const generateMessageIDV2=()=> 'FIXED-ID';const unixTimestampSeconds=()=>1700000000;const WAMessageAddressingMode={LID:'lid',PN:'pn'};const WAMessageStubType={GROUP_PARTICIPANT_ADD:27};const proto={};class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}`
  await writeFile(join(temp,'groups.mjs'),header+stripTypeScriptTypes(source.slice(source.indexOf('export const makeGroupsSocket'))))
  const {makeGroupsSocket,extractGroupMetadata}=await import(pathToFileURL(join(temp,'groups.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
  const n=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),text=(tag,value,attrs={})=>n(tag,attrs,Buffer.from(value)),iq=content=>n('iq',{type:'result'},content)
  const full=n('group',{id:'12345',subject:'Synthetic group 💚',notify:'Group',addressing_mode:'lid',s_o:'900:3@lid',s_o_pn:'100:3@s.whatsapp.net',s_o_username:'owner',s_t:'1700000001',size:'7',creation:'1600000000',creator:'900:2@lid',creator_pn:'100:2@c.us',creator_username:'creator',creator_country_code:'CH'},[
    n('participant',{jid:'900@lid',phone_number:'100@s.whatsapp.net',participant_username:'first',username:'ignored',type:'superadmin'}),n('participant',{jid:'200@s.whatsapp.net',lid:'800@lid',username:'second',type:'admin'}),n('participant',{jid:'300@lid',phone_number:'wrong@lid',lid:'wrong@lid',type:''}),n('description',{id:'previous-description',participant:'900:2@lid',participant_pn:'100:2@c.us',participant_username:'describer',t:'1700000002'},[text('body','Description 💚')]),n('ephemeral',{expiration:'86400'}),n('linked_parent',{jid:'parent@g.us'}),...['locked','announcement','parent','default_sub_group','membership_approval_mode'].map(tag=>n(tag)),text('member_add_mode','all_member_add')])
  const pn=n('group',{id:'67890@g.us',subject:'PN group',s_t:'',creation:'0x10'},[n('participant',{jid:'200@s.whatsapp.net',lid:'800@lid'}),text('member_add_mode','admin_add')])
  const malformed=n('group',{id:'number-cases',s_t:'invalid',size:'invalid',creation:'Infinity'},[n('participant',{}),n('description',{},[text('body','')]),n('ephemeral',{expiration:'invalid'})])
  const metadata=[]
  for(const node of [iq([full]),iq([pn]),iq([malformed]),iq([]),iq([n('group',{subject:'no id'})]),iq([n('error',{code:'403',text:'denied'})]),iq([n('error',{})])]){
    let expected,error;try{expected=extractGroupMetadata(node)}catch(e){error={message:e.message,status:e.output.statusCode}}
    metadata.push({wire:hex(encodeBinaryNode(node)),expected:clean(expected)??null,error:error??null})
  }
  const list=iq([n('groups',{},[full,pn])]),changed=iq([n('add',{},[n('participant',{jid:'100@s.whatsapp.net'}),n('participant',{jid:'200@s.whatsapp.net',error:'403'})])])
  const approval=action=>iq([n('membership_requests_action',{},[n(action,{},[n('participant',{jid:'100@s.whatsapp.net'}),n('participant',{jid:'200@s.whatsapp.net',error:'409'})])])])
  const calls=[['groupMetadata',['12345@g.us'],[iq([full])]],['groupFetchAllParticipating',[],[list]],['groupCreate',['New group',['100@s.whatsapp.net','900@lid']],[iq([full])]],['groupLeave',['12345@g.us'],[iq([])]],['groupUpdateSubject',['12345@g.us','New subject 💚'],[iq([])]],['groupRequestParticipantsList',['12345@g.us'],[iq([n('membership_approval_requests',{},[n('membership_approval_request',{jid:'100@s.whatsapp.net',request_method:'invite_link',t:'1700000000'})])])]],...['approve','reject'].map(action=>['groupRequestParticipantsUpdate',['12345@g.us',['100@s.whatsapp.net','200@s.whatsapp.net'],action],[approval(action)]]),...['add','remove','promote','demote','modify'].map(action=>['groupParticipantsUpdate',['12345@g.us',['100@s.whatsapp.net','200@s.whatsapp.net'],action],[iq([n(action,{},changed.content[0].content)])]]),...['Description changed 💚','',null].map(description=>['groupUpdateDescription',['12345@g.us',description],[iq([full]),iq([])]]),['groupInviteCode',['12345@g.us'],[iq([n('invite',{code:'synthetic-invite'})])]],['groupRevokeInvite',['12345@g.us'],[iq([n('invite',{code:'synthetic-revoked'})])]],['groupAcceptInvite',['synthetic-invite'],[iq([n('group',{jid:'12345@g.us'})])]],['groupRevokeInviteV4',['12345@g.us','100@s.whatsapp.net'],[iq([])]],['groupGetInviteInfo',['synthetic-invite'],[iq([full])]],...[0,86400].map(expiration=>['groupToggleEphemeral',['12345@g.us',expiration],[iq([])]]),...['announcement','not_announcement','locked','unlocked'].map(setting=>['groupSettingUpdate',['12345@g.us',setting],[iq([])]]),...['admin_add','all_member_add'].map(mode=>['groupMemberAddMode',['12345@g.us',mode],[iq([])]]),...['on','off'].map(mode=>['groupJoinApprovalMode',['12345@g.us',mode],[iq([])]])]
  const operations=[]
  for(const [method,args,responses] of calls){const requests=[],events=[],queue=[...responses];const sock={authState:{creds:{}},ev:{emit:(event,data)=>events.push({event,data}),createBufferedFunction:f=>f},ws:{on(){}},query:async node=>{requests.push(node);return queue.shift()},upsertMessage:async()=>{}}
    const expected=await makeGroupsSocket({sock})[method](...args)
    operations.push({method,args,requests:requests.map(n=>hex(encodeBinaryNode(n))),responses:responses.map(n=>hex(encodeBinaryNode(n))),expected:clean(expected)??null,events:clean(events)})
  }
  await writeFile('tests/fixtures/groups.json',JSON.stringify({metadata,operations},null,2)+'\n')
  console.log(`Generated ${metadata.length} metadata cases and ${operations.length} original group operations`)
}finally{await rm(temp,{recursive:true,force:true})}
