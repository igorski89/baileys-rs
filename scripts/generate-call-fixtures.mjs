// Execute pinned call status, handler, listener, ACK builder and five-minute NodeCache.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-calls-'))
try {
 const recv=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),g=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8'),binary=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8'),ack=await readFile(join(upstream,'src/Utils/stanza-ack.ts'),'utf8')
 const handler=recv.slice(recv.indexOf('const handleCall = async ('),recv.indexOf('\n\tconst handleBadAck ='))
 const listener=recv.slice(recv.indexOf("ev.on('call', async ([call]) =>"),recv.indexOf('\n\t/** timestamp of last tctoken prune run'))
 const status=g.slice(g.indexOf('export const getCallStatusFromNode ='),g.indexOf('const UNEXPECTED_SERVER_CODE_TEXT'))
 const helpers=binary.slice(binary.indexOf('const indexCache'),binary.indexOf('export const getBinaryNodeChildBuffer'))
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const nodeCacheUrl=pathToFileURL(require.resolve('@cacheable/node-cache').replace(/\.cjs$/,'.js')).href
 const preamble=`import {NodeCache} from ${JSON.stringify(nodeCacheUrl)};import {proto} from './proto.mjs';const WAMessageStubType=proto.WebMessageInfo.StubType,unixTimestampSeconds=date=>Math.floor(date.getTime()/1000);class Boom extends Error{};const binaryNodeToString=()=>'';`
 const factory=`export function make(failure=null){let clock=1700000000000,listener;const calls=[],messages=[],acks=[],errors=[],cacheTrace=[],actual=new NodeCache({stdTTL:300,useClones:false,checkperiod:0}),logger={error:({error,ackErr})=>errors.push((error??ackErr).message)},callOfferCache={set:async(id,call)=>{cacheTrace.push(['set',id]);if(failure==='set')throw Error('cache set failure');actual.set(id,call)},get:async id=>{cacheTrace.push(['get',id]);if(failure==='get')throw Error('cache get failure');return actual.get(id)},del:async id=>{cacheTrace.push(['delete',id]);if(failure==='delete')throw Error('cache delete failure');actual.del(id)}},sendMessageAck=async n=>{acks.push(buildAckStanza(n,undefined,'100@s.whatsapp.net'))},ev={on:(name,fn)=>{listener=fn},emit:(name,data)=>{calls.push(JSON.parse(JSON.stringify(data.map(call=>{const {date,...rest}=call;return {...rest,dateMs:date.getTime()}}))));listener(data)}},upsertMessage=async(m,kind)=>messages.push({kind,json:proto.WebMessageInfo.toObject(m,{longs:String,enums:Number,bytes:String}),wire:Buffer.from(proto.WebMessageInfo.encode(m).finish()).toString('hex')});`
 const tail=`return {async node(n){const original=Date.now;Date.now=()=>clock;try{await handleCall(n)}finally{Date.now=original}},advance(ms){clock+=ms},result(){return {calls,messages,acks,errors,cacheTrace}},close(){actual.close()}}}`
 await writeFile(join(temp,'calls.mjs'),preamble+stripTypeScriptTypes(helpers+status+ack+factory+handler+listener+tail));const {make}=await import(pathToFileURL(join(temp,'calls.mjs')))
 const node=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),call=(tag,attrs={},root={})=>node('call',{id:'STANZA',from:'200@s.whatsapp.net',t:'1700000000',...root},[node(tag,{'call-id':'CALL','call-creator':'200@s.whatsapp.net',...attrs})]),cases=[]
 async function run(ops,failure=null){const runtime=make(failure);for(const op of ops){if(op.node)await runtime.node(op.node);else runtime.advance(op.advance)}cases.push({ops,failure,expected:runtime.result()});runtime.close()}
 for(const tag of ['offer','offer_notice','preaccept','transport','relaylatency','reject','accept','terminate','unknown'])for(const extra of [{},{reason:'timeout'},{type:'group','group-jid':'group@g.us',caller_pn:'200@s.whatsapp.net'}, {from:'','call-creator':'300@lid','group-jid':''}])for(const time of ['1700000000','-0.0001','invalid','0x10']) {await run([{node:call(tag,extra,{t:time,offline:'0'})}])}
 for(const latency of [{latency:'0'},{latency:'',latency_ms:'-5'},{latency:'NaN',latency_ms:'2'},{'latency-ms':' '},{latency:'Infinity'},{latency:'0x10'},{latency:'12.25'},{latency_ms:'3', 'latency-ms':'4'},{}])await run([{node:call('relaylatency',latency)}])
 for(const video of [false,true])for(const group of [false,true])for(const terminal of ['reject','accept','terminate','timeout'])for(const expiry of [0,300000,300001]) {
 const offer=call('offer',{'caller_pn':'200@s.whatsapp.net',...(group?{'group-jid':'group@g.us'}:{})});if(video)offer.content[0].content=[node('video')]
 await run([{node:offer},{advance:expiry},{node:call('relaylatency',{caller_pn:'',latency:'5'})},{node:call(terminal==='timeout'?'terminate':terminal,terminal==='timeout'?{reason:'timeout'}:{})},{node:call('unknown')}])
 }
 await run([{node:call('offer',{type:'group',caller_pn:'old'})},{node:call('offer',{caller_pn:''})},{node:call('terminate',{reason:'timeout'})}])
 for(const failure of ['set','get','delete'])await run([{node:call('offer')},{node:call('accept')},{node:call('terminate',{reason:'timeout'})}],failure)
 for(const n of [node('call',{id:'MISSING',from:'200@s.whatsapp.net'}),node('call',{id:'EMPTY'},[]),node('call',{},[node('offer')])])await run([{node:n}])
 for(const time of ['8640000000000','8640000000000.001','-8640000000000','Infinity',' ','1.2345'])await run([{node:call('terminate',{reason:'timeout'},{t:time})}])
 const groupOffer=call('offer',{'group-jid':'group@g.us',caller_pn:'200@s.whatsapp.net'},{from:'group@g.us'});groupOffer.content[0].content=[node('video')]
 await run([{node:groupOffer},{node:call('relaylatency',{latency:'5'},{from:'group@g.us',offline:'1'})},{node:call('terminate',{reason:'timeout'},{from:'group@g.us',offline:'1'})},{node:call('unknown',{}, {from:'group@g.us',offline:'1'})}])
 await run([{node:call('offer',{'call-id':'☎️call',type:'group'},{t:'-0.001'})},{node:call('terminate',{'call-id':'☎️call',reason:'timeout'},{t:'-0.001'})}])
 await writeFile('tests/fixtures/calls.json',JSON.stringify({cacheVersion:'1.6.0',cases},null,2)+'\n');console.log(`Generated ${cases.length} complete original call handler/listener streams`)
}finally{await rm(temp,{recursive:true,force:true})}
