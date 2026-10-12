// Execute pinned upstream synchronization orchestration and actual MIT codecs.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-app-sync-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const source=await readFile(join(upstream,'src/Utils/chat-utils.ts'),'utf8'),crypto=await readFile(join(upstream,'src/Utils/crypto.ts'),'utf8'),chats=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8');
 const codecs=source.slice(source.indexOf('const mutationKeys ='),source.indexOf('export const extractSyncdPatches'))+source.slice(source.indexOf('export const decodeSyncdSnapshot'),source.indexOf('export const chatModificationToAppPatch'));
 const aes=crypto.slice(crypto.indexOf('export function aesDecrypt('),crypto.indexOf('export function hmacSign'));
 const loop=chats.slice(chats.indexOf('const resyncAppState ='),chats.indexOf('\n\t/**\n\t * fetch the profile picture'));
 const bridge=pathToFileURL(resolve('.cache/fixture-tools/node_modules/whatsapp-rust-bridge/dist/index.js')).href;
 const prefix=`import {proto} from './proto.mjs';import {expandAppStateKeys,LTHashAntiTampering} from ${JSON.stringify(bridge)};import {createCipheriv,createDecipheriv,createHmac} from 'node:crypto';const randomBytes=n=>Buffer.from(Array.from({length:n},(_,i)=>i)),LT_HASH_ANTI_TAMPERING=new LTHashAntiTampering(),toNumber=v=>typeof v==='object'&&v?v.toNumber():v,hmacSign=(data,key,algorithm='sha256')=>createHmac(algorithm,key).update(data).digest();class Boom extends Error{constructor(text,options){super(text);this.data=options?.data}};`;
 const context=`let authState,query,getAppStateSyncKey,blockedCollections,appStateMacVerification,processed,downloads;const config={options:{}},logger={info(){},warn(){},debug(){},trace(){}},S_WHATSAPP_NET='s.whatsapp.net',ev={createBufferedFunction:f=>f},extractSyncdPatches=async result=>result,newAppStateChunkHandler=()=>({onMutation:m=>processed.push(m)}),downloadExternalPatch=async ref=>{downloads.push(ref.directPath);throw Error('external download failed')};
 export async function run(input){let overlay={...input.initial},committed={...input.initial};const requests=[],calls=[],writes=[];processed=[];downloads=[];blockedCollections=new Set();appStateMacVerification={snapshot:input.validate,patch:input.validate};authState={creds:{},keys:{get:async(type,ids)=>Object.fromEntries(ids.filter(id=>overlay[id]).map(id=>[id,overlay[id]])),set:async data=>{for(const [id,state] of Object.entries(data['app-state-sync-version'])){overlay[id]=state;writes.push(id)}},transaction:async f=>{await f();committed={...overlay}}}};let page=0;query=async node=>{requests.push(node.content[0].content.map(n=>n.attrs));const result=input.pages[page++];if(result?.error)throw Error('query failed');if(!result)throw Error('missing scripted page');return result};getAppStateSyncKey=async id=>{calls.push(id);return input.keyMode==='missing'||id==='CQ=='?undefined:{keyData:Buffer.alloc(32,7)}};let error;try{await resyncAppState(input.names,false)}catch(e){error=e.message}return{requests,calls,writes,states:committed,mutations:processed,downloads,blocked:[...blockedCollections],error};}
 `;
 await writeFile(join(temp,'helpers.mjs'),prefix+stripTypeScriptTypes(aes+codecs)+context+stripTypeScriptTypes(loop));
 const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')));
 const fixture=JSON.parse(await readFile('tests/fixtures/app_state_codec.json','utf8')),good=fixture.batches.find(c=>c.variant==='good'&&c.validate&&c.minimum===null),wire=(t,v)=>Buffer.from(t.encode(v).finish()).toString('hex'),clone=p=>proto.SyncdPatch.decode(proto.SyncdPatch.encode(p).finish()),patches=good.input.map(p=>proto.SyncdPatch.decode(Buffer.from(p,'hex'))),snapshot={version:patches[0].version,records:patches[0].mutations.map(m=>m.record),mac:patches[0].snapshotMac,keyId:patches[0].keyId};
 const initial=(await h.decodePatches('regular',[clone(patches[0])],h.newLTHashState(),async()=>({keyData:Buffer.alloc(32,7)}),{},undefined,undefined,true)).state;
 const response=(ps=[],more=false,snap)=>({regular:{patches:ps,hasMorePatches:more,snapshot:snap}}),missing=clone(patches[0]);missing.keyId={id:Buffer.from([9])};const fatal={...snapshot,records:[{...snapshot.records[0],value:null}]},external=clone(patches[0]);external.externalMutations={directPath:'/fail'};
 const cases=[];
 const state=s=>({version:s.version,hash:Buffer.from(s.hash).toString('hex'),map:Object.fromEntries(Object.entries(s.indexValueMap).map(([k,v])=>[k,Buffer.from(v.valueMac).toString('hex')]))});
 const high=(await h.encodeSyncdPatch({type:'regular_high',index:['mute','300@s.whatsapp.net'],syncAction:proto.SyncActionValue.fromObject({timestamp:0,muteAction:{muted:true}}),apiVersion:3,operation:0},'AQID',h.newLTHashState(),async()=>({keyData:Buffer.alloc(32,7)}))).patch;high.version={version:1};
 for(const validate of [false,true]){
  const badSnapshot={...snapshot,mac:Buffer.from(snapshot.mac)};badSnapshot.mac[0]^=1;
  const defs=[
   {name:'empty',names:[],pages:[]},
   {name:'pagination',pages:[response([clone(patches[0])],true),response([clone(patches[1])],true),response([clone(patches[2])])]},
   {name:'snapshot',pages:[response([],false,snapshot)]},
   {name:'badSnapshotMac',pages:[response([],false,badSnapshot)]},
   {name:'savedVersion',initial:{regular:initial},pages:[response(patches.map(clone))]},
   {name:'deduplicated',names:['regular','regular'],pages:[response([clone(patches[0])])]},
   {name:'missingResponse',pages:[{},response([clone(patches[0])])]},
   {name:'missingKeys',keyMode:'missing',pages:[response([clone(patches[0])]),response([clone(patches[0])])]},
   {name:'snapshotRecovery',pages:[response([clone(missing)]),response([clone(patches[1])],false,snapshot)]},
   {name:'snapshotBeforeBlocked',pages:[response([clone(missing)],false,snapshot),response([clone(missing)])]},
   {name:'queryRollback',pages:[response([clone(patches[0])],true),{error:true}]},
   {name:'fatalInput',pages:[response([],false,fatal)]},
   {name:'downloadRetry',pages:[response([clone(external)]),response([clone(external)])]},
   {name:'unsolicitedPatch',pages:[{regular_high:{patches:[clone(high)],hasMorePatches:false}}]},
   {name:'multipleCollections',names:['regular','regular_high'],pages:[{...response([clone(patches[0])],true),regular_high:{patches:[clone(high)],hasMorePatches:false}},response([clone(patches[1])])]},
   {name:'oneBlockedCollection',names:['regular','regular_high'],pages:[{...response([clone(patches[0])]),regular_high:{patches:[{...clone(high),keyId:{id:Buffer.from([9])}}],hasMorePatches:false}},{regular_high:{patches:[{...clone(high),keyId:{id:Buffer.from([9])}}],hasMorePatches:false}}]},
   ...['firstAggregate','firstPatchMac','middleIndex'].map(variant=>({name:variant,pages:[response(fixture.batches.find(c=>c.variant===variant&&c.validate===validate&&c.minimum===null).input.map(p=>proto.SyncdPatch.decode(Buffer.from(p,'hex'))))]})),
  ];
  for(const def of defs){const input={...def,names:def.names??['regular'],initial:def.initial??{},validate};const pages=input.pages.map(p=>p.error?p:Object.fromEntries(Object.entries(p).map(([name,c])=>[name,{patches:c.patches.map(p=>wire(proto.SyncdPatch,p)),hasMorePatches:c.hasMorePatches,snapshot:c.snapshot?wire(proto.SyncdSnapshot,c.snapshot):null}])));const result=await h.run(input);cases.push({name:def.name,validate,names:input.names,keyMode:input.keyMode??'good',initial:Object.fromEntries(Object.entries(input.initial).map(([k,v])=>[k,state(v)])),pages,expected:{...result,states:Object.fromEntries(Object.entries(result.states).map(([k,v])=>[k,state(v)])),mutations:result.mutations.map(m=>({action:wire(proto.SyncActionData,m.syncAction),index:m.index}))}})}
 }
 await writeFile('tests/fixtures/app_state_sync.json',JSON.stringify(cases,null,2)+'\n');console.log(`Generated ${cases.length} original synchronization scenarios`)
}finally{await rm(temp,{recursive:true,force:true})}
