// Execute the pinned status block and lifecycle resets with a deterministic clock.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-history-status-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const source=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8')
 const status=source.slice(source.indexOf('\t\tif (historyMsg && shouldProcessHistoryMsg)'),source.indexOf('\t\t// State machine: decide on sync and flush'))
 const reset=source.slice(source.indexOf('\t\thistorySyncStatus.initialBootstrapComplete = false'),source.indexOf('\n\t\tsyncState = SyncState.AwaitingInitialSync',source.indexOf('\t\thistorySyncStatus.initialBootstrapComplete = false')))
 const close=source.slice(source.indexOf('\t\t\tclearTimeout(historySyncPausedTimeout)',source.indexOf("if (connection === 'close')")),source.indexOf('\n\t\t}',source.indexOf("if (connection === 'close')")))
 await writeFile(join(temp,'status.mjs'),`import {proto} from './proto.mjs';const HISTORY_SYNC_PAUSED_TIMEOUT_MS=120000;export function make(){const historySyncStatus={initialBootstrapComplete:false,recentSyncComplete:false},events=[],ev={emit:(name,data)=>events.push(data)};let historySyncPausedTimeout,pendingSeen=false;return {observe(historyMsg,shouldProcessHistoryMsg){`+stripTypeScriptTypes(status)+`},pending(){if(!pendingSeen){pendingSeen=true;`+stripTypeScriptTypes(reset)+`}},close(){pendingSeen=false;`+stripTypeScriptTypes(close)+`},state(){return {...historySyncStatus}},events};}`)
 const {make}=await import(pathToFileURL(join(temp,'status.mjs')))
 const observe=(syncType,progress,eligible=true)=>({op:'observe',notification:{...(syncType===null?{}:{syncType}),...(progress===null?{}:{progress})},eligible}),advance=now=>({op:'advance',now})
 const streams=[
  [observe(0,null),observe(0,100),observe(3,100),observe(3,99),advance(120000)],
  [observe(3,50),advance(119999),advance(120000),observe(3,100),observe(3,50),advance(240000)],
  [observe(3,0),advance(110000),observe(3,99),advance(120000),advance(230000)],
  [observe(3,50),{op:'pending'},advance(120000),observe(0,0),{op:'pending'},observe(0,100)],
  [observe(0,null),observe(3,0),{op:'close'},advance(120000),{op:'pending'},observe(0,null),observe(3,100)],
  [observe(0,100,false),observe(3,100,false),observe(null,100),observe(2,100),advance(120000)],
 ]
 let seed=17;const random=n=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed%n}
 for(let i=0;i<100;i++){let now=0;const operations=[];for(let j=0;j<40;j++){switch(random(6)){case 0:operations.push({op:'pending'});break;case 1:operations.push({op:'close'});break;case 2:case 3:operations.push(observe([null,0,1,2,3,4,6,99][random(8)],[null,0,50,99,100,101][random(6)],!!random(4)));break;default:now+=random(150001);operations.push(advance(now));}}streams.push(operations)}
 const originalSet=globalThis.setTimeout,originalClear=globalThis.clearTimeout,cases=[]
 for(const operations of streams){let now=0,next=1;const timers=new Map();globalThis.setTimeout=(fn,delay)=>{const id=next++;timers.set(id,{at:now+delay,fn});return id};globalThis.clearTimeout=id=>timers.delete(id);const tracker=make(),steps=[];
  for(const operation of operations){switch(operation.op){case'observe':tracker.observe(operation.notification,operation.eligible);break;case'pending':tracker.pending();break;case'close':tracker.close();break;case'advance':while(true){const first=[...timers].filter(([,t])=>t.at<=operation.now).sort((a,b)=>a[1].at-b[1].at||a[0]-b[0])[0];if(!first)break;timers.delete(first[0]);now=first[1].at;first[1].fn()}now=operation.now;break;}steps.push({state:tracker.state(),deadline:[...timers.values()][0]?.at??null,events:tracker.events.splice(0)})}cases.push({operations,steps});}
 globalThis.setTimeout=originalSet;globalThis.clearTimeout=originalClear;
 await writeFile('tests/fixtures/history_status.json',JSON.stringify({cases},null,2)+'\n');console.log(`Generated ${cases.length} source history-progress streams`)
}finally{await rm(temp,{recursive:true,force:true})}
