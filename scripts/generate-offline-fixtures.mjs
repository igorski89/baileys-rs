import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-offline-'))
try {
 const source=await readFile(join(upstream,'src/Utils/offline-node-processor.ts'),'utf8')
 await writeFile(join(temp,'offline.mjs'),stripTypeScriptTypes(source))
 const {makeOfflineNodeProcessor}=await import(pathToFileURL(join(temp,'offline.mjs')))
 const cases=[]
 const item=(type,id,extra={})=>({type,node:{tag:type,attrs:{id,...extra}}})
 async function run(batch,phases,failYield=false){let open=true;const trace=[],snapshots=[],handlers=new Map(['message','call','receipt','notification'].map(type=>[type,async node=>{trace.push(['handle',type,node.attrs.id]);if(node.attrs.close)open=false;if(node.attrs.crash)throw Error('handler failure')}]))
 const processor=makeOfflineNodeProcessor(handlers,{isWsOpen:()=>open,onUnexpectedError:(err,context)=>trace.push(['error',context,err.message]),yieldToEventLoop:async()=>{trace.push(['yield']);if(failYield)throw Error('yield failure')}},batch)
 for(const phase of phases){if(phase.open!==undefined)open=phase.open;for(const {type,node}of phase.items)processor.enqueue(type,node);await new Promise(setImmediate);snapshots.push(structuredClone(trace))}
 cases.push({batch,phases,failYield,expected:snapshots})}
 for(const batch of [0,1,3,10])for(const size of [0,1,2,3,9,10,11,20,25])for(const errors of [false,true]) {
 const items=Array.from({length:size},(_,i)=>item(['message','call','receipt','notification'][i%4],String(i),errors&&i%3===1?{crash:'1'}:{}));if(errors)items.splice(1,0,item('unknown','unknown'))
 await run(batch,[{items}])}
 for(const batch of [1,3,10]) {
 await run(batch,[{open:false,items:[item('message','a'),item('receipt','b')]},{open:true,items:[]},{items:[item('notification','c')]}])
 await run(batch,[{items:[item('message','a',{close:'1'}),item('receipt','b'),item('call','c')]},{open:true,items:[]},{items:[item('notification','d')]}])
 await run(batch,[{items:[item('message','a'),item('receipt','b'),item('call','c'),item('notification','d')]},{items:[item('message','e')]}],true)
 }
 await writeFile('tests/fixtures/offline.json',JSON.stringify({cases},null,2)+'\n');console.log(`Generated ${cases.length} complete original offline processor streams`)
}finally{await rm(temp,{recursive:true,force:true})}
