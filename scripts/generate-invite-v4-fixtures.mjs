// Run the MIT-licensed Baileys group and community V4 invite acceptance with query and upsert stubs.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-invite-v4-')),hex=b=>Buffer.from(b).toString('hex')
try {
  for(const name of ['constants','jid-utils','encode','generic-utils']){
    let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
    s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}")
    s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
    await writeFile(join(temp,`${name}.mjs`),s)
  }
  await writeFile(join(temp,'proto.mjs'),(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
  const header=`import {getBinaryNodeChild,getBinaryNodeChildren,getBinaryNodeChildString} from './generic-utils.mjs';import {isLidUser,isPnUser,jidEncode,jidNormalizedUser} from './jid-utils.mjs';import {proto} from './proto.mjs';const makeChatsSocket=c=>c.sock,makeBusinessSocket=c=>c.sock,generateMessageID=()=>'FIXED-ID',generateMessageIDV2=()=>'FIXED-ID',unixTimestampSeconds=()=>1700000000,WAMessageAddressingMode={LID:'lid',PN:'pn'},WAMessageStubType=proto.WebMessageInfo.StubType,logger={info(){}};class Boom extends Error{constructor(message,options){super(message);this.output={statusCode:options?.statusCode??500}}}`
  for(const [file,start] of [['groups','export const makeGroupsSocket'],['communities','export const makeCommunitiesSocket']]){
    const source=await readFile(join(upstream,`src/Socket/${file}.ts`),'utf8')
    await writeFile(join(temp,`${file}.mjs`),header+stripTypeScriptTypes(source.slice(source.indexOf(start))))
  }
  const {makeGroupsSocket}=await import(pathToFileURL(join(temp,'groups.mjs'))),{makeCommunitiesSocket}=await import(pathToFileURL(join(temp,'communities.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')))
  const n=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})})
  const me={id:'100:7@s.whatsapp.net',name:'Me 💚',lid:'900:7@lid'}
  const invite={groupJid:'12345@g.us',inviteCode:'INVITE-CODE',inviteExpiration:'1700600000',groupName:'Group 💚',caption:'Join us',jpegThumbnail:Buffer.from([255,216,1,2]),groupType:0}
  const admin='300@s.whatsapp.net',full={remoteJid:admin,fromMe:false,id:'INVITE-MSG-1'}
  const inputs=[
    [admin,invite],[full,invite],[{remoteJid:admin},invite],[{...full,participant:'400@lid'},invite],[{id:'ONLY-ID'},invite],[{remoteJid:admin,id:''},invite],
    [full,{groupJid:'12345@g.us'}],[admin,{groupJid:'12345@g.us',inviteCode:'',inviteExpiration:0}],[full,{inviteCode:'NO-GROUP',inviteExpiration:5}],[full,{...invite,inviteExpiration:'9007199254740993',groupType:1}]
  ]
  const cases=[]
  for(const [kind,make] of [['group',makeGroupsSocket],['community',makeCommunitiesSocket]])for(const [key,message] of inputs)for(const response of [n('iq',{type:'result',from:message.groupJid??'joined@g.us'}),n('iq',{type:'result'})]){
    const requests=[],events=[],upserts=[],queue=[response]
    const sock={user:me,authState:{creds:{me}},ev:{emit:(name,data)=>events.push({name,data}),createBufferedFunction:f=>f},ws:{on(){}},query:async node=>{requests.push(node);return queue.shift()},upsertMessage:async(m,type)=>upserts.push({type,wire:hex(proto.WebMessageInfo.encode(proto.WebMessageInfo.fromObject(m)).finish())})}
    const decoded=proto.Message.GroupInviteMessage.decode(proto.Message.GroupInviteMessage.encode(proto.Message.GroupInviteMessage.fromObject(message)).finish())
    let expected,error=false;try{expected=await make({sock,logger:{info(){}}})[kind+'AcceptInviteV4'](typeof key==='string'?key:{...key},decoded)}catch(e){error=true}
    cases.push({kind,key,invite:hex(proto.Message.GroupInviteMessage.encode(proto.Message.GroupInviteMessage.fromObject(message)).finish()),requests:requests.map(r=>hex(encodeBinaryNode(r))),response:hex(encodeBinaryNode(response)),
      updates:events.map(e=>{if(e.name!=='messages.update')throw Error('unexpected event '+e.name);return e.data.map(u=>({key:u.key,message:hex(proto.Message.encode(proto.Message.fromObject(u.update.message)).finish())}))}),upserts,expected:expected??null,error})
  }
  await writeFile('tests/fixtures/invite-v4.json',JSON.stringify({me,cases},null,1)+'\n')
  console.log(`Generated ${cases.length} original V4 invite acceptance cases`)
}finally{await rm(temp,{recursive:true,force:true})}
