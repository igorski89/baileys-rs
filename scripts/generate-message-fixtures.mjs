// Runtime oracle from the pinned MIT upstream, using synthetic message data only.
import {stripTypeScriptTypes, createRequire} from 'node:module'
import {readFile, writeFile, mkdtemp, rm} from 'node:fs/promises'
import {join, resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-messages-'))
const hex=b=>Buffer.from(b).toString('hex')
try {
  let protoSource=await readFile(join(upstream,'WAProto/index.js'),'utf8')
  protoSource=protoSource.replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp,'proto.mjs'),protoSource)
  for(const name of ['constants','jid-utils','encode','decode','generic-utils']) {
    let source=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8')
    source=source.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains = {'+body.replaceAll('=',':')+'}')
    source=stripTypeScriptTypes(source).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replaceAll("'../../WAProto/index.js'","'./proto.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error {}")
    await writeFile(join(temp,`${name}.mjs`),source)
  }
  const generic=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8')
  const padding=generic.slice(generic.indexOf('export const writeRandomPadMax16'),generic.indexOf('// code is inspired'))
  const ids=generic.slice(generic.indexOf('export const generateMessageIDV2'),generic.indexOf('export function bindWaitForEvent'))
  await writeFile(join(temp,'generics.mjs'),stripTypeScriptTypes('import {createHash} from "node:crypto";import {jidDecode} from "./jid-utils.mjs";let random=0;const randomBytes=n=>Buffer.alloc(n,random);export function setRandom(n){random=n};'+padding+ids))
  let decode=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/decode-wa-message.ts'),'utf8')).replaceAll("'../../WAProto/index.js'","'./proto.mjs'").replaceAll("'../WABinary'","'./jid-utils.mjs'").replaceAll("'./generics'","'./generics.mjs'").replace("import { Boom } from '@hapi/boom'","class Boom extends Error {}")
  await writeFile(join(temp,'messages.mjs'),decode)
  const {proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs'))),{decodeMessageNode,decryptMessageNode}=await import(pathToFileURL(join(temp,'messages.mjs'))),generics=await import(pathToFileURL(join(temp,'generics.mjs')))
  const {decodeBinaryNode}=await import(pathToFileURL(join(temp,'decode.mjs')))
  const me='100:1@s.whatsapp.net',lid='900:1@lid',make=attrs=>({tag:'message',attrs:{id:'ABC',from:'200:7@s.whatsapp.net',t:'1700000000',...attrs}})
  const attrs=[{}, {from:'100:2@s.whatsapp.net'}, {from:'900:3@lid',recipient:'200@s.whatsapp.net',recipient_pn:'200@s.whatsapp.net'}, {from:'100:2@s.whatsapp.net',recipient:'200@s.whatsapp.net',sender_lid:'900@lid',peer_recipient_username:'sample'}, {from:'123@g.us',participant:'900:3@lid',participant_pn:'100@s.whatsapp.net',participant_username:'alice'}, {from:'123@g.us',participant:'200:7@s.whatsapp.net',participant_lid:'800@lid',addressing_mode:'pn'}, {from:'status@broadcast',participant:'100:2@s.whatsapp.net'}, {from:'status@broadcast',participant:'900:2@lid'}, {from:'123@broadcast',participant:'200@s.whatsapp.net'}, {from:'123@newsletter',server_id:'567',notify:'News'}, {from:'200:99@hosted'}, {from:'800:99@hosted.lid',sender_pn:'200@s.whatsapp.net'}, {recipient:'123@bot'}, {from:'200@s.whatsapp.net',recipient:'300@s.whatsapp.net'}, {id:''}, {from:''}, {from:'123@g.us'}, {from:'123@unknown'}]
  const metadata=message=>({remoteJidAlt:message.key.remoteJidAlt??null,remoteJidUsername:message.key.remoteJidUsername??null,participantAlt:message.key.participantAlt??null,participantUsername:message.key.participantUsername??null,addressingMode:message.key.addressingMode,isViewOnce:message.key.isViewOnce??false,serverId:message.key.server_id??null})
  const envelopes=attrs.map(attrs=>{const node=make(attrs);try{const decoded=decodeMessageNode(node,me,lid);return {wire:hex(encodeBinaryNode(node)),message:hex(proto.WebMessageInfo.encode(decoded.fullMessage).finish()),metadata:metadata(decoded.fullMessage),author:decoded.author,sender:decoded.sender}}catch(error){return {wire:hex(encodeBinaryNode(node)),error:error.message}}})
  const payload=obj=>proto.Message.encode(obj).finish(),plain=obj=>({tag:'plaintext',attrs:{},content:payload(obj)}),encrypted=(type,obj)=>({tag:'enc',attrs:{type,count:'2'},content:Buffer.concat([payload(obj),Buffer.from([1])])})
  const cert=proto.VerifiedNameCertificate.encode({details:proto.VerifiedNameCertificate.Details.encode({verifiedName:'Synthetic business'}).finish()}).finish()
  const cases=[
    [plain({conversation:'hello'})],
    [{tag:'enc',attrs:{type:'plaintext'},content:payload({conversation:'enc plaintext'})}],
    [plain({deviceSentMessage:{destinationJid:'300@s.whatsapp.net',message:{conversation:'wrapped'}}})],
    [plain({extendedTextMessage:{text:'old',title:'remove me'},conversation:'first'}),plain({extendedTextMessage:{text:'new'},conversation:''})],
    [encrypted('pkmsg',{senderKeyDistributionMessage:{groupId:'123@g.us',axolotlSenderKeyDistributionMessage:Buffer.from([1,2,3])}}),encrypted('skmsg',{conversation:'group'})],
    [encrypted('msg',{conversation:'ratchet'}),{tag:'verified_name',attrs:{},content:cert}],
    [],[{tag:'unavailable',attrs:{type:'view_once'}}],
    [{tag:'enc',attrs:{type:'unknown'},content:Buffer.from([1])}],
    [{tag:'plaintext',attrs:{},content:Buffer.from([255])}],
    [plain({conversation:'preserved despite following error'}),{tag:'enc',attrs:{type:'unknown'},content:Buffer.from([1])}],
    [{tag:'enc',attrs:{type:'msg'},content:'not bytes'}]
  ]
  const decryptions=[]
  for(const children of cases){const wire=encodeBinaryNode({...make({}),content:children}),stanza=await decodeBinaryNode(wire),calls=[];const repository={lidMapping:{async getLIDForPN(){return null}},async decryptMessage(data){calls.push({kind:data.type,author:data.jid});return data.ciphertext},async decryptGroupMessage(data){calls.push({kind:'skmsg',group:data.group,author:data.authorJid});return data.msg},async processSenderKeyDistributionMessage(data){calls.push({kind:'distribution',author:data.authorJid,group:data.item.groupId})}};const logger={debug(){},warn(){},error(){}};const result=decryptMessageNode(stanza,me,lid,repository,logger);await result.decrypt();decryptions.push({wire:hex(wire),message:hex(proto.WebMessageInfo.encode(result.fullMessage).finish()),metadata:metadata(result.fullMessage),retryCount:result.fullMessage.retryCount??null,calls})}
  const oldNow=Date.now;Date.now=()=>1700000000123;generics.setRandom(7)
  const idsFixture=[undefined,'100@s.whatsapp.net','100:7@lid','123456789012345678901234567890@s.whatsapp.net','💚@s.whatsapp.net','invalid'].map(user=>({user:user??null,id:generics.generateMessageIDV2(user)}));Date.now=oldNow
  const pads=Array.from({length:32},(_,n)=>{generics.setRandom(n);return {byte:n,wire:hex(generics.writeRandomPadMax16(Buffer.from([1,2,3])))}})
  await writeFile('tests/fixtures/messages.json',JSON.stringify({me,lid,envelopes,decryptions,ids:idsFixture,pads},null,2)+'\n')
  console.log(`Generated ${envelopes.length} envelopes, ${decryptions.length} payload cases and ID/padding fixtures`)
}finally{await rm(temp,{recursive:true,force:true})}
