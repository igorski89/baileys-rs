// Run the MIT-licensed Baileys community socket operations and metadata parser with query stubs.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-communities-')),hex=b=>Buffer.from(b).toString('hex')
const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):v
try {
  for(const name of ['constants','jid-utils','encode','generic-utils']){
    let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
    s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}")
    s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
    await writeFile(join(temp,`${name}.mjs`),s)
  }
  const source=await readFile(join(upstream,'src/Socket/communities.ts'),'utf8')
  const header=`import {getBinaryNodeChild,getBinaryNodeChildren,getBinaryNodeChildString} from './generic-utils.mjs';import {jidEncode,jidNormalizedUser} from './jid-utils.mjs';const makeBusinessSocket=c=>c.sock;let counter=0;const generateMessageID=()=>'3EB0FIXEDIDFIXEDID'+(counter++),generateMessageIDV2=()=>'FIXED-ID',unixTimestampSeconds=()=>1700000000,WAMessageStubType={GROUP_PARTICIPANT_ADD:27},proto={},logger={info(){}};const console={error(){}};`
  await writeFile(join(temp,'communities.mjs'),header+stripTypeScriptTypes(source.slice(source.indexOf('export const makeCommunitiesSocket'))))
  const {makeCommunitiesSocket,extractCommunityMetadata}=await import(pathToFileURL(join(temp,'communities.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
  const n=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),text=(tag,value,attrs={})=>n(tag,attrs,Buffer.from(value)),iq=content=>n('iq',{type:'result'},content)
  const full=n('community',{id:'12345',subject:'Synthetic community 💚',s_o:'900:3@lid',s_t:'1700000001',creation:'1600000000',creator:'900:2@lid'},[n('participant',{jid:'900@lid',type:'superadmin'}),n('participant',{jid:'200@s.whatsapp.net',type:'admin'}),n('participant',{jid:'300@lid',type:''}),n('description',{id:'previous-description'},[text('body','Description 💚')]),n('ephemeral',{expiration:'86400'}),n('linked_parent',{jid:'parent@g.us'}),...['locked','announcement','parent','default_sub_community','membership_approval_mode'].map(tag=>n(tag)),text('member_add_mode','all_member_add'),text('addressing_mode','lid')])
  const plain=n('community',{id:'67890@g.us',s_t:'',creation:'0x10'},[n('participant',{jid:'200@s.whatsapp.net'}),text('member_add_mode','admin_add')])
  const malformed=n('community',{s_t:'invalid',creation:'Infinity',subject:''},[n('participant',{}),n('description',{},[text('body','')]),n('ephemeral',{expiration:'invalid'}),n('linked_parent',{jid:''})])
  const metadata=[]
  for(const node of [iq([full]),iq([plain]),iq([malformed]),iq([n('community',{id:'zero'},[n('ephemeral',{expiration:'0'}),n('ephemeral',{})])]),iq([])]){
    let expected,error=false;try{expected=extractCommunityMetadata(node)}catch(e){error=true}
    metadata.push({wire:hex(encodeBinaryNode(node)),expected:clean(expected)??null,error})
  }
  const group=n('group',{id:'555',subject:'Sub group',s_t:'1700000003',creation:'1700000002',creator:'900@lid'},[n('participant',{jid:'900@lid',type:'admin'}),n('linked_parent',{jid:'12345@g.us'})])
  const list=iq([n('communities',{},[full,plain])]),changed=[n('participant',{jid:'100@s.whatsapp.net'}),n('participant',{jid:'200@s.whatsapp.net',error:'403'})]
  const approval=action=>iq([n('membership_requests_action',{},[n(action,{},[n('participant',{jid:'100@s.whatsapp.net'}),n('participant',{jid:'200@s.whatsapp.net',error:'409'})])])])
  const subGroups=iq([n('sub_groups',{},[n('group',{id:'555',subject:'Sub group',creation:'1700000002',creator:'900:1@lid',size:'3'}),n('group',{}),n('other',{id:'x'})])])
  const calls=[['communityMetadata',['12345@g.us'],[iq([full])]],['communityFetchAllParticipating',[],[list]],['communityFetchAllParticipating',[],[iq([])]],
   ['communityCreate',['New community','Body 💚'],[iq([n('group',{id:'555'})]),iq([group])]],['communityCreate',['No body',''],[iq([])]],['communityCreateGroup',['New sub group',['100@s.whatsapp.net','900@lid'],'12345@g.us'],[iq([n('group',{id:'555'})]),iq([group])]],
   ['communityLeave',['12345@g.us'],[iq([])]],['communityUpdateSubject',['12345@g.us','New subject 💚'],[iq([])]],['communityLinkGroup',['555@g.us','12345@g.us'],[iq([])]],['communityUnlinkGroup',['555@g.us','12345@g.us'],[iq([])]],
   ['communityFetchLinkedGroups',['555@g.us'],[iq([group]),subGroups]],['communityFetchLinkedGroups',['12345@g.us'],[iq([n('group',{id:'12345',subject:'Parent',s_t:'1',creation:'2'},[n('parent')])]),iq([])]],
   ['communityRequestParticipantsList',['12345@g.us'],[iq([n('membership_approval_requests',{},[n('membership_approval_request',{jid:'100@s.whatsapp.net',request_method:'invite_link',t:'1700000000'})])])]],['communityRequestParticipantsList',['12345@g.us'],[iq([])]],
   ...['approve','reject'].map(action=>['communityRequestParticipantsUpdate',['12345@g.us',['100@s.whatsapp.net','200@s.whatsapp.net'],action],[approval(action)]]),
   ...['add','remove','promote','demote','modify'].map(action=>['communityParticipantsUpdate',['12345@g.us',['100@s.whatsapp.net','200@s.whatsapp.net'],action],[iq([n(action,{},changed)])]]),
   ...['Description changed 💚','',null].map(description=>['communityUpdateDescription',['12345@g.us',description],[iq([full]),iq([])]]),['communityUpdateDescription',['67890@g.us','First description'],[iq([plain]),iq([])]],
   ['communityInviteCode',['12345@g.us'],[iq([n('invite',{code:'synthetic-invite'})])]],['communityInviteCode',['12345@g.us'],[iq([])]],['communityRevokeInvite',['12345@g.us'],[iq([n('invite',{code:'synthetic-revoked'})])]],['communityAcceptInvite',['synthetic-invite'],[iq([n('community',{jid:'12345@g.us'})])]],['communityAcceptInvite',['synthetic-invite'],[iq([])]],
   ['communityRevokeInviteV4',['12345@g.us','100@s.whatsapp.net'],[iq([])]],['communityGetInviteInfo',['synthetic-invite'],[iq([full])]],...[0,86400].map(expiration=>['communityToggleEphemeral',['12345@g.us',expiration],[iq([])]]),
   ...['announcement','not_announcement','locked','unlocked'].map(setting=>['communitySettingUpdate',['12345@g.us',setting],[iq([])]]),...['admin_add','all_member_add'].map(mode=>['communityMemberAddMode',['12345@g.us',mode],[iq([])]]),...['on','off'].map(mode=>['communityJoinApprovalMode',['12345@g.us',mode],[iq([])]])]
  // The group metadata dependency is exercised by its own fixtures; here it reports what it was asked and replays a response.
  const groupMetadata=n=>{const g=n.content?.find(c=>c.tag==='group');if(!g)throw Error('missing group');return {id:g.attrs.id.includes('@')?g.attrs.id:g.attrs.id+'@g.us',subject:g.attrs.subject,linkedParent:g.content?.find(c=>c.tag==='linked_parent')?.attrs.jid||undefined}}
  const operations=[]
  for(const [method,args,responses] of calls){const requests=[],events=[],queue=[...responses];const query=async node=>{requests.push(node);return queue.shift()}
    const sock={authState:{creds:{}},ev:{emit:(event,data)=>events.push({event,data}),createBufferedFunction:f=>f},ws:{on(){}},query,upsertMessage:async()=>{},groupMetadata:async jid=>groupMetadata(await query(n('iq',{type:'get',xmlns:'w:g2',to:jid},[n('query',{request:'interactive'})])))}
    let expected,error=false;try{expected=await makeCommunitiesSocket({sock})[method](...args)}catch(e){error=true}
    operations.push({method,args,requests:requests.map(n=>hex(encodeBinaryNode(n))),responses:responses.map(n=>hex(encodeBinaryNode(n))),expected:clean(expected)??null,error,events:clean(events)})
  }
  await writeFile('tests/fixtures/communities.json',JSON.stringify({metadata,operations},null,2)+'\n')
  console.log(`Generated ${metadata.length} metadata cases and ${operations.length} original community operations`)
}finally{await rm(temp,{recursive:true,force:true})}
