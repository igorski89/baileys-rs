// Run the MIT-licensed Baileys message, node and version helpers that are exported for callers rather than used by the socket.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {createHash} from 'node:crypto'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-utility-')),hex=b=>Buffer.from(b).toString('hex')
try {
  await writeFile(join(temp,'proto.mjs'),(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
  const boom=JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)
  for(const name of ['constants','jid-utils','encode','generic-utils']){
    let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
    s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("'@hapi/boom'",boom)
    await writeFile(join(temp,`${name}.mjs`),s.replace("'../../WAProto/index.js'","'./proto.mjs'"))
  }
  const slice=(s,from,to)=>s.slice(s.indexOf(from),s.indexOf(to))
  const messages=await readFile(join(upstream,'src/Utils/messages.ts'),'utf8'),generics=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8'),media=await readFile(join(upstream,'src/Utils/messages-media.ts'),'utf8')
  await writeFile(join(temp,'helpers.mjs'),`import {createHash} from 'crypto';import {Boom} from ${boom};const sha256=b=>createHash('sha256').update(b).digest(),baileysVersion=[2,3000,1043857760];`
    +stripTypeScriptTypes(slice(generics,'export const getKeyAuthor','export const isStringNullOrEmpty')+slice(generics,'export const fetchLatestBaileysVersion','/** unique message tag prefix')+slice(generics,'export const isWABusinessPlatform','// eslint-disable-next-line @typescript-eslint/no-explicit-any\nexport function trimUndefined')
    +slice(messages,'export const getDevice','/** Given a list of message keys')+slice(media,'export function extensionForMediaMessage','const isNodeRuntime')))
  const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs'))),g=await import(pathToFileURL(join(temp,'generic-utils.mjs')))
  const info=o=>proto.WebMessageInfo.fromObject({key:{remoteJid:'chat@s.whatsapp.net',id:'M',fromMe:false},...o}),wire=m=>hex(proto.WebMessageInfo.encode(proto.WebMessageInfo.fromObject(m)).finish())
  const devices=['3A0123456789ABCDEF01','3E0123456789ABCDEF0123','3EB0123456789ABCDEF012','012345678901234567890','0123456789ABCDEF0123456789ABCDEF','3F01','3FABCDEFABCDEFABCDEFABCDEFABCDEFAB','012345678901234567','3A012345678901234567X','','BAE5ABCDEF012345','3A0123456789ABCDEF0💚','3A\n123456789ABCDEF01'].map(id=>({id,expected:h.getDevice(id)}))
  // Each sequence applies its updates in order to one message; the message is recorded after every step.
  const run=(start,steps,apply,encode)=>{const message=info(start);return {start:wire(message),steps:steps.map(step=>{apply(message,structuredClone(step));return {update:encode(step),expected:wire(message)}})}}
  const key=(remoteJid,participant,fromMe=false,id='R')=>({remoteJid,id,fromMe,...(participant===undefined?{}:{participant})})
  const receipts=[[{},[{userJid:'a@s.whatsapp.net',receiptTimestamp:10},{userJid:'b@s.whatsapp.net',readTimestamp:20},{userJid:'a@s.whatsapp.net',readTimestamp:30},{userJid:'a@s.whatsapp.net',receiptTimestamp:40,playedTimestamp:50},{userJid:'',receiptTimestamp:1}]],
    [{userReceipt:[{userJid:'x@lid',receiptTimestamp:1,readTimestamp:2,playedTimestamp:3}]},[{userJid:'x@lid'},{userJid:'x@lid',readTimestamp:0},{userJid:'X@lid',receiptTimestamp:9}]]]
    .map(([start,steps])=>run(start,steps,h.updateMessageWithReceipt,s=>hex(proto.UserReceipt.encode(proto.UserReceipt.fromObject(s)).finish())))
  const reactions=[[{},[{key:key('chat@s.whatsapp.net'),text:'👍',senderTimestampMs:1},{key:key('chat@s.whatsapp.net'),text:'💚',senderTimestampMs:2},{key:key('g@g.us','p1@lid'),text:'a'},{key:key('g@g.us','p2@lid'),text:'b'},{key:key('g@g.us','p1@lid')},{key:key('other@s.whatsapp.net',undefined,true),text:'me'},{key:key('else@s.whatsapp.net',undefined,true),text:'me again'},{text:'no key'},{key:{},text:'empty key'},{key:key('g@g.us','')}]],
    [{reactions:[{key:key('a@s.whatsapp.net'),text:'old'},{key:key('b@s.whatsapp.net'),text:'keep'},{key:key('a@s.whatsapp.net'),text:'duplicate'}]},[{key:key('a@s.whatsapp.net'),text:''}]]]
    .map(([start,steps])=>run(start,steps,h.updateMessageWithReaction,s=>hex(proto.Reaction.encode(proto.Reaction.fromObject(s)).finish())))
  const sha=name=>createHash('sha256').update(name).digest(),vote=(k,options,extra={})=>({pollUpdateMessageKey:k,...(options?{vote:{selectedOptions:options}}:{}),...extra})
  const pollSteps=[vote(key('g@g.us','p1@lid'),[sha('One')],{senderTimestampMs:1}),vote(key('g@g.us','p2@lid'),[sha('One'),sha('Two 💚')]),vote(key('g@g.us','p1@lid'),[sha('Two 💚')]),vote(key('g@g.us','p2@lid'),[]),vote(key('g@g.us','p3@lid')),vote(key('g@g.us',undefined,true),[sha('Missing'),sha('One')]),vote(undefined,[sha('')]),vote(key('g@g.us','p1@lid'),[Buffer.from('raw')])]
  const pollUpdates=[[{},pollSteps],[{pollUpdates:[vote(key('g@g.us','p9@lid'),[sha('One')]),vote(key('g@g.us','p9@lid'),[sha('Two 💚')])]},[vote(key('g@g.us','p9@lid'),[sha('One')])]]]
    .map(([start,steps])=>run(start,steps,h.updateMessageWithPollUpdate,s=>hex(proto.PollUpdate.encode(proto.PollUpdate.fromObject(s)).finish())))
  const options=names=>({options:names.map(optionName=>optionName===null?{}:{optionName})}),updates=pollSteps.filter(u=>u.vote)
  const votes=[]
  for(const message of [{pollCreationMessage:options(['One','Two 💚','Three'])},{pollCreationMessageV2:options(['One','One','',null])},{pollCreationMessageV3:options(['Two 💚'])},{pollCreationMessage:options(['First']),pollCreationMessageV3:options(['One'])},{pollCreationMessage:options([]),pollCreationMessageV2:options(['One'])},{conversation:'not a poll'},null])
    for(const pollUpdates of [updates,[],[vote(key('g@g.us','p1@lid'),[sha('Missing'),sha('Missing')]),vote(key('g@g.us','p2@lid'),[sha('Missing')])]])for(const me of [undefined,'100@s.whatsapp.net']){
      const input=info({...(message?{message}:{}),pollUpdates})
      votes.push({message:wire(input),me:me??null,expected:h.getAggregateVotesInPollMessage(input,me)})
    }
  // Upstream reads the answer from a runtime property that the protobuf type does not have.
  const answer=(k,eventResponse)=>({eventResponseMessageKey:k,...(eventResponse===undefined?{}:{eventResponse})}),responseLists=[[],[answer(key('g@g.us','p1@lid'),'GOING'),answer(key('g@g.us','p2@lid'),'NOT_GOING'),answer(key('g@g.us','p3@lid'),'MAYBE'),answer(key('g@g.us','p4@lid'),'GOING'),answer(key('g@g.us',undefined,true),'GOING'),answer(key('g@g.us','p5@lid'),'UNKNOWN'),answer(key('g@g.us','p6@lid')),answer(undefined,'MAYBE'),answer(key('g@g.us','p1@lid'),'GOING')]]
  const responses=responseLists.flatMap(list=>[undefined,'100@s.whatsapp.net'].map(me=>({responses:list.map(r=>({key:r.eventResponseMessageKey?hex(proto.MessageKey.encode(proto.MessageKey.fromObject(r.eventResponseMessageKey)).finish()):null,response:r.eventResponse??null})),me:me??null,expected:h.getAggregateResponsesInEventMessage({eventResponses:list},me)})))
  const responseSteps=[[{},[{eventResponseMessageKey:key('g@g.us','p1@lid'),timestampMs:1,eventResponseMessage:{response:1}},{eventResponseMessageKey:key('g@g.us','p2@lid'),eventResponseMessage:{response:3,extraGuestCount:2}},{eventResponseMessageKey:key('g@g.us','p1@lid'),timestampMs:5,eventResponseMessage:{response:2}},{timestampMs:7},{eventResponseMessageKey:key('g@g.us',undefined,true)},{eventResponseMessageKey:key('h@g.us',undefined,true),unread:true}]]]
    .map(([start,steps])=>run(start,steps,h.updateMessageWithEventResponse,s=>hex(proto.EventResponse.encode(proto.EventResponse.fromObject(s)).finish())))
  const extensions=[{locationMessage:{}},{liveLocationMessage:{}},{productMessage:{}},{imageMessage:{mimetype:'image/jpeg'}},{videoMessage:{mimetype:'video/mp4; codecs=avc1'}},{audioMessage:{mimetype:'audio/ogg; codecs=opus'}},{documentMessage:{mimetype:'application/vnd.ms-excel;x=1;y=2'}},{stickerMessage:{mimetype:'image/webp'}},{ptvMessage:{mimetype:'video/mp4'}},{imageMessage:{mimetype:'jpeg'}},{imageMessage:{mimetype:''}},{imageMessage:{mimetype:';image/png'}},{imageMessage:{mimetype:'a/b/c'}},{imageMessage:{mimetype:'image/'}},{imageMessage:{}},{conversation:'text'},{extendedTextMessage:{text:'x'}},{}]
    .map(m=>{const message=proto.Message.fromObject(m),plain=proto.Message.toObject(message);let expected=null,error=false;try{expected=h.extensionForMediaMessage(plain)??null}catch{error=true}return {message:hex(proto.Message.encode(message).finish()),expected,error}})
  const platforms=['smbi','smba','android','smb','SMBA','',' smba','iphone'].map(platform=>({platform,expected:h.isWABusinessPlatform(platform)}))
  const pages=[['baileys',200,'1\n2\n3\n4\n5\n6\nconst version = [2, 3000, 1023456789]\n8'],['baileys',200,'1\n2\n3\n4\n5\n6\nconst version = [2,3000,7] as const'],['baileys',200,'1\n2\n3\n4\n5\n6\n  const version = [02,  3000,\t4294967295]'],['baileys',200,'const version = [2, 3000, 1]\n2\n3\n4\n5\n6\nother'],['baileys',200,'short'],['baileys',200,'1\n2\n3\n4\n5\n6\nconst version = [2, 3000]'],['baileys',404,'1\n2\n3\n4\n5\n6\nconst version = [2, 3000, 1]'],['baileys',200,'1\r\n2\r\n3\r\n4\r\n5\r\n6\r\nconst version = [1, 2, 3]\r\n'],
    ['web',200,'x "client_revision":1023456789, y'],['web',200,'x \\"client_revision\\": 42 y \\"client_revision\\":43'],['web',200,'"client_revision" : 5'],['web',200,'"client_revision":\n\t 0077'],['web',200,'client_revision:5'],['web',200,''],['web',500,'"client_revision":1'],['web',200,'\\"client_revision":12abc'],['web',200,'"client_revision\\":4294967295']]
  const versions=[]
  for(const [kind,status,text] of pages){globalThis.fetch=async()=>({ok:status>=200&&status<300,status,statusText:'Status '+status,text:async()=>text});const r=await (kind==='baileys'?h.fetchLatestBaileysVersion:h.fetchLatestWaWebVersion)();versions.push({kind,status,text,expected:{version:r.version,isLatest:r.isLatest}})}
  const n=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})})
  const trees=[n('iq'),n('iq',{type:'get',id:'1.2-3'}),n('iq',{to:'s.whatsapp.net'},[n('a',{x:'1'}),n('b',{},[n('c',{},[n('d',{y:"it's"})])])]),n('message',{},Buffer.from([0,1,254,255])),n('m',{id:'1'},[n('text',{},Buffer.from('hi 💚')),n('empty',{},[]),n('raw',{},Buffer.alloc(0))]),n('s',{},'plain text'),n('s',{a:''},[n('t',{},'nested text')]),n('s',{},''),n('deep',{},[n('1',{},[n('2',{},[n('3',{},[n('4',{},Buffer.from('ff','hex'))])])])])]
  // Trees are recorded structurally: the wire encoding does not keep text content apart from bytes.
  const tree=({tag,attrs,content})=>({tag,attrs:Object.entries(attrs),content:content===undefined?null:Array.isArray(content)?{children:content.map(tree)}:typeof content==='string'?{text:content}:{bytes:hex(content)}})
  const strings=trees.map(node=>({node:tree(node),expected:g.binaryNodeToString(node)}))
  const props=n('props',{},[n('prop',{name:'a',value:'1'}),n('prop',{name:'b',config_value:'2'}),n('prop',{config_code:'77',value:'3'}),n('prop',{config_code:'78',config_value:'4'}),n('prop',{name:'c',value:'',config_value:'fallback'}),n('prop',{name:'a',value:'again'}),n('other',{name:'z',value:'no'}),n('prop',{name:'d',config_code:'9',value:'named'}),n('prop',{name:'10',value:'numeric'}),n('prop',{config_code:'2',value:'low'})])
  const dictionaries=[[props,'prop'],[props,'other'],[props,'none'],[n('props'),'prop'],[n('props',{},Buffer.from('x')),'prop']].map(([node,tag])=>({node:hex(encodeBinaryNode(node)),tag,expected:Object.entries(g.reduceBinaryNodeToDictionary(node,tag))}))
  const stanza=n('result',{},[n('message',{},proto.WebMessageInfo.encode(info({message:{conversation:'one'}})).finish()),n('other',{},Buffer.from('x')),n('message',{},proto.WebMessageInfo.encode(info({message:{conversation:'two 💚'},messageTimestamp:5})).finish())])
  const nodeMessages=[{node:hex(encodeBinaryNode(stanza)),expected:g.getBinaryNodeMessages(stanza).map(m=>hex(proto.WebMessageInfo.encode(proto.WebMessageInfo.fromObject(m)).finish()))},{node:hex(encodeBinaryNode(n('result'))),expected:[]}]
  await writeFile('tests/fixtures/utilities.json',JSON.stringify({devices,receipts,reactions,pollUpdates,votes,responses,responseSteps,extensions,platforms,versions,strings,dictionaries,nodeMessages},null,1)+'\n')
  console.log(`Generated ${devices.length} device, ${receipts.length+reactions.length+pollUpdates.length+responseSteps.length} update sequence, ${votes.length} vote, ${responses.length} response, ${extensions.length} extension, ${versions.length} version, ${strings.length} string, ${dictionaries.length} dictionary cases`)
}finally{await rm(temp,{recursive:true,force:true})}
