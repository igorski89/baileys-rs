// Execute pinned MIT record/patch/snapshot codecs with original Node CBC crypto.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {createHmac} from 'node:crypto'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-app-codec-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const source=await readFile(join(upstream,'src/Utils/chat-utils.ts'),'utf8'),crypto=await readFile(join(upstream,'src/Utils/crypto.ts'),'utf8');
 const codecs=source.slice(source.indexOf('const mutationKeys ='),source.indexOf('export const extractSyncdPatches'))+source.slice(source.indexOf('export const decodeSyncdSnapshot'),source.indexOf('export const chatModificationToAppPatch'));
 let code=stripTypeScriptTypes(codecs).replace('const mutationKeys','export const mutationKeys').replace('const generateMac','export const generateMac').replace('const generateSnapshotMac','export const generateSnapshotMac').replace('const generatePatchMac','export const generatePatchMac');
 const aes=crypto.slice(crypto.indexOf('export function aesDecrypt('),crypto.indexOf('export function hmacSign'));const bridge=pathToFileURL(resolve('.cache/fixture-tools/node_modules/whatsapp-rust-bridge/dist/index.js')).href;
 const prefix=`import {proto} from './proto.mjs';import {expandAppStateKeys,LTHashAntiTampering} from ${JSON.stringify(bridge)};import {createCipheriv,createDecipheriv,createHmac} from 'node:crypto';const randomBytes=n=>Buffer.from(Array.from({length:n},(_,i)=>i)),LT_HASH_ANTI_TAMPERING=new LTHashAntiTampering(),toNumber=v=>typeof v==='object'&&v?v.toNumber():v,hmacSign=(data,key,algorithm='sha256')=>createHmac(algorithm,key).update(data).digest();class Boom extends Error{constructor(text,options){super(text);this.data=options?.data}};`;
 await writeFile(join(temp,'helpers.mjs'),prefix+stripTypeScriptTypes(aes)+code);const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs'))),hex=b=>Buffer.from(b).toString('hex'),wire=(type,v)=>hex(type.encode(v).finish()),state=s=>({version:s.version,hash:hex(s.hash),map:Object.fromEntries(Object.entries(s.indexValueMap).map(([k,v])=>[k,hex(v.valueMac)]))}),mutation=m=>({action:wire(proto.SyncActionData,m.syncAction),index:m.index}),secret=Buffer.alloc(32,7),id=Buffer.from([1,2,3]),id64=id.toString('base64');
 function provider(mode,calls){return async key=>{calls.push(key);if(mode==='missing'||key==='CQ==')return undefined;if(mode==='error')throw Error('temporary key lookup failure');if(mode==='malformed')return {};return {keyData:secret}}}
 const encodings=[];
 for(const operation of [0,1,99])for(const version of [0,3,4294967294,4294967295])for(const index of [['mute','200@s.whatsapp.net'],['Unicode💚','é','\u2028']])for(const prev of [false,true]){
  const action=proto.SyncActionValue.fromObject({timestamp:0,archiveChatAction:{archived:true}}),request={type:'regular',index,syncAction:action,apiVersion:3,operation},initial=h.newLTHashState();initial.version=version;
  if(prev){const k=h.mutationKeys(secret),indexMac=hmacSync(Buffer.from(JSON.stringify(index)),k.indexKey),g=h.makeLtHashGenerator(initial);g.mix({indexMac,valueMac:Buffer.alloc(32,9),operation:0});Object.assign(initial,g.finish())}
  const input=state(initial),calls=[];let result,error;try{result=await h.encodeSyncdPatch(request,id64,initial,provider('good',calls))}catch(e){error=e.message}encodings.push({operation,index,version,prev,input,action:wire(proto.SyncActionValue,action),patch:result?wire(proto.SyncdPatch,result.patch):null,state:result?state(result.state):null,error,calls,original:state(initial)})
 }
 function hmacSync(data,key){return createHmac('sha256',key).update(data).digest()}
 const good=await h.encodeSyncdPatch({type:'regular',index:['mute','200@s.whatsapp.net'],syncAction:proto.SyncActionValue.fromObject({timestamp:12,muteAction:{muted:true}}),apiVersion:3,operation:0},id64,h.newLTHashState(),provider('good',[]));
 const decodings=[];const variants=['good','valueMac','indexMac','cbc','protobuf','json','noKeyId','noValue','noIndex','missingKey','shortValue','callbackError'];
 for(const variant of variants)for(const validate of [false,true])for(const mode of ['good','missing','error','malformed']){
  const record=proto.SyncdRecord.decode(proto.SyncdRecord.encode(good.patch.mutations[0].record).finish()),k=h.mutationKeys(secret);
  if(variant==='valueMac')record.value.blob[record.value.blob.length-1]^=1;if(variant==='indexMac')record.index.blob[0]^=1;
  if(['cbc','protobuf','json'].includes(variant)){const plain=variant==='protobuf'?Buffer.from([255]):proto.SyncActionData.encode({index:Buffer.from('invalid json'),version:3}).finish();let enc=variant==='cbc'?Buffer.alloc(16):h.aesEncrypt(plain,k.valueEncryptionKey);record.value.blob=Buffer.concat([enc,h.generateMac(0,enc,id,k.valueMacKey)]);if(variant==='json')record.index.blob=hmacSync(Buffer.from('invalid json'),k.indexKey)}
  if(variant==='noKeyId')record.keyId=null;if(variant==='noValue')record.value=null;if(variant==='noIndex')record.index=null;if(variant==='missingKey')record.keyId={id:Buffer.from([9])};if(variant==='shortValue')record.value.blob=Buffer.from([1]);
  const records=[good.patch.mutations[0].record,record,good.patch.mutations[0].record],calls=[],mutations=[];let result,error,missing=false;try{result=await h.decodeSyncdMutations(records,h.newLTHashState(),provider(mode,calls),m=>{mutations.push(mutation(m));if(variant==='callbackError'&&mutations.length===2)throw Error('callback failed')},validate)}catch(e){error=e.message;missing=h.isMissingKeyError(e)}
  decodings.push({variant,validate,mode,records:records.map(r=>wire(proto.SyncdRecord,r)),state:result?state({...result,version:0}):null,mutations,calls,error,missing})
 }
 const aggregate=[];
 for(const kind of ['snapshot','patch'])for(const variant of ['good','valueMac','indexMac','aggregateMac','mainMac','missingKey'])for(const validate of [false,true])for(const minimum of [null,0,1,2]){
  const patch=proto.SyncdPatch.decode(proto.SyncdPatch.encode(good.patch).finish());patch.version={version:1};if(variant==='valueMac')patch.mutations[0].record.value.blob[patch.mutations[0].record.value.blob.length-1]^=1;if(variant==='indexMac')patch.mutations[0].record.index.blob[0]^=1;if(variant==='aggregateMac')patch.snapshotMac[0]^=1;if(variant==='mainMac')patch.patchMac[0]^=1;
  // Keep patch MAC valid for records intentionally corrupted at a lower layer.
  if(variant!=='mainMac')patch.patchMac=h.generatePatchMac(patch.snapshotMac,patch.mutations.map(m=>m.record.value.blob.slice(-32)),1,'regular',h.mutationKeys(secret).patchMacKey);
  const snapshot={version:{version:1},records:patch.mutations.map(m=>m.record),mac:patch.snapshotMac,keyId:patch.keyId},calls=[],warnings=[],logger={warn:(...v)=>warnings.push(String(v[v.length-1]))};let result,error,missing=false;try{result=kind==='snapshot'?await h.decodeSyncdSnapshot('regular',snapshot,provider(variant==='missingKey'?'missing':'good',calls),minimum??undefined,validate,logger):await h.decodePatches('regular',[patch],h.newLTHashState(),provider(variant==='missingKey'?'missing':'good',calls),{},minimum??undefined,logger,validate)}catch(e){error=e.message;missing=h.isMissingKeyError(e)}
  aggregate.push({kind,variant,validate,minimum,input:wire(kind==='snapshot'?proto.SyncdSnapshot:proto.SyncdPatch,kind==='snapshot'?snapshot:proto.SyncdPatch.decode(Buffer.from(wire(proto.SyncdPatch,{...patch,mutations:snapshot.records.map(record=>({operation:0,record}))}),'hex'))),state:result?state(result.state):null,mutations:result?Object.fromEntries(Object.entries(result.mutationMap).map(([key,value])=>[key,mutation(value)])):null,calls,warnings,error,missing,cleared:patch.mutations.length===0})
 }
 const batches=[];
 for(const variant of ['good','firstPatchMac','middleIndex','firstAggregate','duplicateIndexes','missingKey'])for(const validate of [false,true])for(const minimum of [null,1,3]){
  let initial=h.newLTHashState();const patches=[];for(let i=0;i<3;i++){const encoded=await h.encodeSyncdPatch({type:'regular',index:['mute',variant==='duplicateIndexes'?'same':String(i)],syncAction:proto.SyncActionValue.fromObject({timestamp:i,muteAction:{muted:true}}),apiVersion:3,operation:0},id64,initial,provider('good',[]));initial=encoded.state;encoded.patch.version={version:i+1};patches.push(proto.SyncdPatch.decode(proto.SyncdPatch.encode(encoded.patch).finish()))}
  if(variant==='firstPatchMac')patches[0].patchMac[0]^=1;
  if(variant==='middleIndex')patches[1].mutations[0].record.index.blob[0]^=1;
  if(variant==='firstAggregate'){patches[0].snapshotMac[0]^=1;patches[0].patchMac=h.generatePatchMac(patches[0].snapshotMac,patches[0].mutations.map(m=>m.record.value.blob.slice(-32)),1,'regular',h.mutationKeys(secret).patchMacKey)}
  const input=patches.map(p=>wire(proto.SyncdPatch,p)),calls=[],warnings=[];let result,error,missing=false;try{result=await h.decodePatches('regular',patches,h.newLTHashState(),provider(variant==='missingKey'?'missing':'good',calls),{},minimum??undefined,{warn:(...v)=>warnings.push(String(v[v.length-1]))},validate)}catch(e){error=e.message;missing=h.isMissingKeyError(e)}
  batches.push({variant,validate,minimum,input,state:result?state(result.state):null,mutations:result?Object.fromEntries(Object.entries(result.mutationMap).map(([k,v])=>[k,mutation(v)])):null,calls,warnings,error,missing,remaining:patches.map(p=>p.mutations.length)})
 }
 await writeFile('tests/fixtures/app_state_codec.json',JSON.stringify({secret:hex(secret),id:id64,encodings,decodings,aggregate,batches},null,2)+'\n');console.log(`Generated ${encodings.length} encodings, ${decodings.length} record streams and ${aggregate.length} aggregate decodes, ${batches.length} patch sequences`)
}finally{await rm(temp,{recursive:true,force:true})}
