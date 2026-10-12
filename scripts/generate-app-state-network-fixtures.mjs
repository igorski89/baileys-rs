// Execute the pinned upstream collection extractor with synthetic blob transport.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-app-network-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const source=await readFile(join(upstream,'src/Utils/chat-utils.ts'),'utf8'),binary=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8');
 const accessors=binary.slice(binary.indexOf('const indexCache'),binary.indexOf('export const getBinaryNodeChildBuffer'));
 const extract=source.slice(source.indexOf('export const extractSyncdPatches'),source.indexOf('export const downloadExternalBlob'));
 await writeFile(join(temp,'helpers.mjs'),`import {proto} from './proto.mjs';export const downloads=[];const downloadExternalBlob=async ref=>{downloads.push(ref.directPath);return proto.SyncdSnapshot.encode({version:{version:9},records:[],mac:Buffer.from([1]),keyId:{id:Buffer.from([2])}}).finish()};`+stripTypeScriptTypes(accessors+extract));
 const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')));
 const wire=(t,v)=>Buffer.from(t.encode(v).finish()), node=(tag,attrs={},content)=>({tag,attrs,content}), native=n=>({tag:n.tag,attrs:n.attrs,content:n.content===undefined?null:Array.isArray(n.content)?{Children:n.content.map(native)}:{Bytes:Array.from(n.content)}});
 const cases=[];
 for(const wrapped of [false,true])for(const snapshot of [false,true])for(const version of ['0','12','4294967295'])for(const more of ['true','false','TRUE']){
  const patches=[node('patch',{},wire(proto.SyncdPatch,{})),node('patch',{},wire(proto.SyncdPatch,{version:{version:7}})),node('patch')];
  const children=wrapped?[node('patches',{},patches)]:patches;
  if(snapshot)children.push(node('snapshot',{},wire(proto.ExternalBlobReference,{directPath:'/snapshot',mediaKey:Buffer.alloc(32,8)})));
  const input=node('iq',{},[node('sync',{},[node('collection',{name:'regular',version,has_more_patches:more},children)])]);
  h.downloads.length=0;const result=await h.extractSyncdPatches(input,{});
  cases.push({input:native(input),downloads:[...h.downloads],expected:Object.fromEntries(Object.entries(result).map(([name,c])=>[name,{patches:c.patches.map(p=>wire(proto.SyncdPatch,p).toString('hex')),hasMorePatches:c.hasMorePatches,snapshot:c.snapshot?wire(proto.SyncdSnapshot,c.snapshot).toString('hex'):null}]))});
 }
 await writeFile('tests/fixtures/app_state_network.json',JSON.stringify(cases,null,2)+'\n');console.log(`Generated ${cases.length} original collection cases`)
}finally{await rm(temp,{recursive:true,force:true})}
