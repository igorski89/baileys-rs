// Execute original server_sync dispatch and notification finally/ACK behavior.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-sync-notify-'))
try {
 const source=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),generic=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8');
 await writeFile(join(temp,'ack.mjs'),stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/stanza-ack.ts'),'utf8')));
 const branch=source.slice(source.indexOf("case 'server_sync':"),source.indexOf("case 'picture':",source.indexOf("case 'server_sync':"))),handler=source.slice(source.indexOf('const handleNotification ='),source.indexOf('const handleMessage ='));
 const accessors=generic.slice(generic.indexOf('const indexCache'),generic.indexOf('export const getBinaryNodeChildBuffer'));
 const body=`export async function run(node,fail,ackFail){const trace=[],logger={error(){}},notificationMutex={mutex:async work=>work()},resyncAppState=async(names,initial)=>{trace.push({operation:'sync',names,initial});if(fail)throw Error('sync failed')},sendMessageAck=async node=>{trace.push({operation:'ack',node:buildAckStanza(node)});if(ackFail)throw Error('ack failed')},processNotification=async node=>{switch(node.attrs.type){${branch}}};${handler}let error;try{await handleNotification(node)}catch(e){error=e.message}return{trace,error};}`;
 await writeFile(join(temp,'helpers.mjs'),`import {buildAckStanza} from './ack.mjs';\n`+stripTypeScriptTypes(accessors+body));const h=await import(pathToFileURL(join(temp,'helpers.mjs')));
 const cases=[];
 for(const shape of ['absent','text','single','multiple','emptyName'])for(const fail of [false,true])for(const ackFail of [false,true])for(const metadata of [false,true]){
  const node={tag:'notification',attrs:{id:'NOTIFICATION',from:'s.whatsapp.net',type:'server_sync',...(metadata?{participant:'100:2@s.whatsapp.net',recipient:'100@lid',offline:'true'}:{})},content:shape==='absent'?undefined:shape==='text'?'text':[{tag:'collection',attrs:{name:shape==='emptyName'?'':'regular',version:'999'}},...(shape==='multiple'?[{tag:'collection',attrs:{name:'ignored'}}]:[])]};
  const result=await h.run(node,fail,ackFail),native=n=>({tag:n.tag,attrs:n.attrs,content:n.content===undefined?null:Array.isArray(n.content)?{Children:n.content.map(native)}:{Text:n.content}});
  cases.push({shape,fail,ackFail,node:native(node),expected:{error:result.error,trace:result.trace.map(t=>t.node?{...t,node:native(t.node)}:t)}});
 }
 await writeFile('tests/fixtures/app_state_notifications.json',JSON.stringify(cases,null,2)+'\n');console.log(`Generated ${cases.length} original notification dispatch cases`)
}finally{await rm(temp,{recursive:true,force:true})}
