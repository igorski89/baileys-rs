// Execute pinned MIT app-state helpers and their actual Rust/WASM bridge.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-app-foundation-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const source=await readFile(join(upstream,'src/Utils/chat-utils.ts'),'utf8'),start=source.indexOf('const mutationKeys ='),end=source.indexOf('export const ensureLTHashStateVersion');
 const bridge=pathToFileURL(resolve('.cache/fixture-tools/node_modules/whatsapp-rust-bridge/dist/index.js')).href;
 let code=stripTypeScriptTypes(source.slice(start,end)).replace('const mutationKeys','export const mutationKeys').replace('const generateMac','export const generateMac').replace('const generateSnapshotMac','export const generateSnapshotMac').replace('const generatePatchMac','export const generatePatchMac');
 const prefix=`import {proto} from './proto.mjs';import {expandAppStateKeys,LTHashAntiTampering} from ${JSON.stringify(bridge)};import {createHmac} from 'node:crypto';const LT_HASH_ANTI_TAMPERING=new LTHashAntiTampering(),hmacSign=(data,key,algorithm='sha256')=>createHmac(algorithm,key).update(data).digest();`;
 await writeFile(join(temp,'helpers.mjs'),prefix+code);const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),b=await import(bridge),lt=new b.LTHashAntiTampering(),hex=value=>Buffer.from(value).toString('hex'),bytes=(n,seed)=>Buffer.from(Array.from({length:n},(_,i)=>(i*17+seed)%256)),state=s=>({hash:hex(s.hash),indexValueMap:Object.fromEntries(Object.entries(s.indexValueMap).map(([k,v])=>[k,hex(v.valueMac)]))});
 const keys=[];for(const length of [0,1,16,32,64,128]){const secret=bytes(length,3),k=h.mutationKeys(secret);keys.push({secret:hex(secret),keys:Object.fromEntries(Object.entries(k).map(([name,value])=>[name,hex(value)]))})}
 const arithmetic=[];for(const seed of [0,1,255])for(const count of [0,1,2,5,32])for(const size of [0,1,32,64]){const base=Buffer.alloc(128,seed),subtract=Array.from({length:count},(_,i)=>bytes(size,i)),add=Array.from({length:count+1},(_,i)=>bytes(size,i+3)),output=lt.subtractThenAdd(base,subtract,add);arithmetic.push({base:hex(base),subtract:subtract.map(hex),add:add.map(hex),output:hex(output)})}
 const generators=[];for(const count of [0,1,2,5,32,257])for(const seed of [0,4,255]){
  const indexes=Array.from({length:5},(_,i)=>bytes(32,i)),initial={version:17,hash:bytes(128,seed),indexValueMap:{[indexes[0].toString('base64')]:{valueMac:bytes(32,7)}}},input=state(initial),generator=h.makeLtHashGenerator(initial),mutations=[];
  for(let i=0;i<count;i++){const op={indexMac:indexes[i%5],valueMac:bytes(i%3===0?0:32,i),operation:[0,1,99,0,1][i%5]};generator.mix(op);mutations.push({index:hex(op.indexMac),value:hex(op.valueMac),operation:op.operation})}
  generators.push({input,mutations,output:state(generator.finish()),repeat:state(generator.finish()),originalAfter:state(initial)})
 }
 const macs=[];for(const operation of [0,1,99])for(const length of [0,1,32,254,255,256,300])for(const version of [0,1,4294967295,4294967296])for(const name of ['regular','critical_block','Unicode💚']){
  const key=bytes(32,9),id=bytes(length,2),data=bytes(17,3),mac=h.generateMac(operation,data,id,key);let snapshot,patch,error;try{snapshot=h.generateSnapshotMac(bytes(128,4),version,name,key);patch=h.generatePatchMac(snapshot,[mac,bytes(32,5)],version,name,key)}catch(e){error=e.message}macs.push({operation,key:hex(key),id:hex(id),data:hex(data),value:hex(mac),version,name,snapshot:snapshot?hex(snapshot):null,patch:patch?hex(patch):null,error})
 }
 await writeFile('tests/fixtures/app_state_foundation.json',JSON.stringify({keys,arithmetic,generators,macs},null,2)+'\n');console.log(`Generated ${keys.length} derivations, ${arithmetic.length} LT arithmetic, ${generators.length} mutation streams and ${macs.length} MAC cases`)
}finally{await rm(temp,{recursive:true,force:true})}
