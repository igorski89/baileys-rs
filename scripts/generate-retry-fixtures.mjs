// Execute the pinned MIT upstream retry manager with a controlled clock and real LRU.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'), require=createRequire(resolve('.cache/fixture-tools/package.json'))
let clock=1000
const originalDate=Date.now, originalPerformance=globalThis.performance
Date.now=()=>clock;globalThis.performance={now:()=>clock}
const {LRUCache}=require('lru-cache')
// Disable the LRU clock's one-millisecond memoization so each synthetic step sees its clock.
globalThis.RetryFixtureLRU=class extends LRUCache { constructor(options){super({...options,ttlResolution:0})} }
const temp=await mkdtemp(join(tmpdir(),'baileys-retry-'))
try {
  let source=await readFile(join(upstream,'src/Utils/message-retry-manager.ts'),'utf8')
  source=source.replace("import { LRUCache } from 'lru-cache'",'const LRUCache=globalThis.RetryFixtureLRU')
  source=source.replace('private logger: ILogger','logger: ILogger').replace('this.maxMsgRetryCount = maxMsgRetryCount','this.logger = logger; this.maxMsgRetryCount = maxMsgRetryCount')
  source=source.replace(/export enum RetryReason \{([\s\S]*?)\n\}/,(_,body)=>{
    const entries=[...body.matchAll(/(\w+)\s*=\s*(\d+)/g)].flatMap(([,name,num])=>[`${name}:${num}`,`${num}:'${name}'`]);return `export const RetryReason={${entries.join(',')}}`
  })
  await writeFile(join(temp,'retry.mjs'),stripTypeScriptTypes(source))
  const {MessageRetryManager}=await import(pathToFileURL(join(temp,'retry.mjs')))
  const manager=new MessageRetryManager({debug(){},warn(){}},3)
  const operations=[
    {op:'parse',values:[null,'',' ','wat','0','4','7foo',' +13tail','14','-1','-0','0x7','1.9','\uFEFF7','999999999999999999999999999','\u00857']},
    {op:'recreate',jid:'x',session:false},{op:'recreate',jid:'x',session:true},{op:'recreate',jid:'x',session:true,error:4},{op:'recreate',jid:'x',session:true,error:7},{op:'advance',ms:3600000},{op:'recreate',jid:'x',session:true},{op:'advance',ms:1},{op:'recreate',jid:'x',session:true},{op:'stats'},
    {op:'clear'},{op:'add',to:'A',id:'dup',text:'A'},{op:'add',to:'B',id:'dup',text:'B'},{op:'fillRecent',count:511},{op:'success',id:'dup'},{op:'get',to:'B',id:'dup'},{op:'get',to:'A',id:'dup'},
    {op:'clear'},{op:'add',to:'A',id:'a',text:'old'},{op:'add',to:'A',id:'a',text:'new'},{op:'get',to:'A',id:'a'},{op:'failed',id:'a'},{op:'get',to:'A',id:'a'},
    {op:'add',to:'A',id:'ttl',text:'ttl'},{op:'advance',ms:300000},{op:'get',to:'A',id:'ttl'},{op:'advance',ms:1},{op:'get',to:'A',id:'ttl'},
    {op:'increment',id:'counter'},{op:'increment',id:'counter'},{op:'exceeded',id:'counter'},{op:'increment',id:'counter'},{op:'exceeded',id:'counter'},
    {op:'advance',ms:800000},{op:'count',id:'counter'},{op:'advance',ms:800000},{op:'count',id:'counter'},{op:'advance',ms:900001},{op:'count',id:'counter'},
    {op:'base',address:'a:b',id:'c',key:[1,2]},{op:'sameBase',address:'a',id:'b:c',key:[1,2]},{op:'sameBase',address:'a:b',id:'c',key:[1]},{op:'sameBase',address:'a:b',id:'c',key:[1,3]},
    {op:'fillBase',count:1024},{op:'sameBase',address:'a:b',id:'c',key:[1,2]},{op:'base',address:'x',id:'ttl',key:[]},{op:'advance',ms:900001},{op:'sameBase',address:'x',id:'ttl',key:[]},
    {op:'base',address:'x',id:'del',key:[4]},{op:'deleteBase',address:'x',id:'del'},{op:'sameBase',address:'x',id:'del',key:[4]},
    {op:'stats'},{op:'clear'},{op:'stats'},{op:'recreate',jid:'new',session:true},{op:'advance',ms:7200001},{op:'recreate',jid:'new',session:true},
    {op:'clear'},{op:'clock',ms:0},{op:'recreate',jid:'zero',session:true},{op:'recreate',jid:'zero',session:true},
    {op:'add',to:'A\u0000B',id:'C',text:'first'},{op:'add',to:'A',id:'B\u0000C',text:'second'},{op:'get',to:'A\u0000B',id:'C'},{op:'success',id:'C'},{op:'get',to:'A',id:'B\u0000C'},
    {op:'base',address:'boundary',id:'key',key:[8]},{op:'advance',ms:800000},{op:'sameBase',address:'boundary',id:'key',key:[8]},{op:'advance',ms:100001},{op:'sameBase',address:'boundary',id:'key',key:[8]},
    {op:'base',address:'normalBoundary',id:'key',key:[8]},{op:'advance',ms:800000},{op:'sameBase',address:'normalBoundary',id:'key',key:[8]},{op:'advance',ms:100001},{op:'sameBase',address:'normalBoundary',id:'key',key:[8]}
  ]
  const cases=[]
  for(const step of operations){let result=null
    switch(step.op){
      case 'parse':result=step.values.map(v=>{const reason=manager.parseRetryErrorCode(v??undefined);return {reason:reason??null,mac:manager.isMacError(reason)}});break
      case 'advance':clock+=step.ms;break
      case 'clock':clock=step.ms;break
      case 'recreate':result=manager.shouldRecreateSession(step.jid,step.session,step.error);break
      case 'add':manager.addRecentMessage(step.to,step.id,{conversation:step.text});break
      case 'fillRecent':for(let i=0;i<step.count;i++)manager.addRecentMessage('fill',`m${i}`,{conversation:`${i}`});break
      case 'get':result=manager.getRecentMessage(step.to,step.id)??null;break
      case 'increment':result=manager.incrementRetryCount(step.id);break
      case 'count':result=manager.getRetryCount(step.id);break
      case 'exceeded':result=manager.hasExceededMaxRetries(step.id);break
      case 'success':manager.markRetrySuccess(step.id);break
      case 'failed':manager.markRetryFailed(step.id);break
      case 'base':manager.saveBaseKey(step.address,step.id,Uint8Array.from(step.key));break
      case 'sameBase':result=manager.hasSameBaseKey(step.address,step.id,Uint8Array.from(step.key));break
      case 'fillBase':for(let i=0;i<step.count;i++)manager.saveBaseKey('fill',`${i}`,Uint8Array.from([i%256]));break
      case 'deleteBase':manager.deleteBaseKey(step.address,step.id);break
      case 'stats':result=manager.statistics;break
      case 'clear':manager.clear();break
    }
    cases.push({step,result:JSON.parse(JSON.stringify(result))})
  }
  manager.clear()
  await writeFile('tests/fixtures/retry.json',JSON.stringify({start:1000,max:3,cases},null,2)+'\n')
  console.log(`Generated ${cases.length} retry-manager operations from upstream`)
}finally{Date.now=originalDate;globalThis.performance=originalPerformance;delete globalThis.RetryFixtureLRU;await rm(temp,{recursive:true,force:true})}
