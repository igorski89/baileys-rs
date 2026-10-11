// Run the upstream migration method with synthetic v1 session records.
import {stripTypeScriptTypes, createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-migration-'))
try {
  let jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8')
  jid=jid.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains = {'+body.replaceAll('=',':')+'}')
  await writeFile(join(temp,'jid.mjs'),stripTypeScriptTypes(jid))
  const source=await readFile(join(upstream,'src/Signal/libsignal.ts'),'utf8')
  const method=source.slice(source.indexOf('async migrateSession('),source.indexOf('\n\t}\n\n\treturn repository'))
  const addressing=source.slice(source.indexOf('const jidToSignalProtocolAddress ='),source.indexOf('const jidToSignalSenderKeyName ='))
  const prefix=`import libsignal from ${JSON.stringify(pathToFileURL(require.resolve('libsignal')).href)};import {LRUCache} from ${JSON.stringify(pathToFileURL(require.resolve('lru-cache')).href)};import {jidDecode,transferDevice,isPnUser,isHostedPnUser,isLidUser,isHostedLidUser,WAJIDDomains} from './jid.mjs';`
  await writeFile(join(temp,'migration.mjs'),prefix+stripTypeScriptTypes(addressing+'export function make(parsedKeys){const logger={debug(){}};const migratedSessionCache=new LRUCache({ttl:3*24*60*60*1000,ttlAutopurge:true,updateAgeOnGet:true});return {close(){migratedSessionCache.clear()},'+method+'}}'))
  const {make}=await import(pathToFileURL(join(temp,'migration.mjs')))
  const direct=JSON.parse(await readFile('tests/fixtures/direct.json','utf8')),base=direct.initial.alice
  const record=(registration,closed=false)=>{const value=structuredClone(base);const session=Object.values(value._sessions)[0];session.registrationId=registration;if(closed)session.indexInfo.closed=123;return value}
  const cases=[
    {name:'open-closed-missing-and-requested',data:{'device-list':{'100':['0','1','2','3']},session:{'100.0':record(10),'100.1':record(11,true),'100.3':record(13),'100.7':record(17),'900_1.0':record(99)},'identity-key':{'100.0':[5,1,2]}},operations:[['100:7@s.whatsapp.net','900@lid'],['100:7@s.whatsapp.net','900@lid']]},
    {name:'no-device-list',data:{session:{'100.7':record(17)}},operations:[['100:7@s.whatsapp.net','900@lid']]},
    {name:'empty-device-list-requested',data:{'device-list':{'100':[]},session:{'100.7':record(17)}},operations:[['100:7@s.whatsapp.net','900@lid']]},
    {name:'invalid-directions',data:{},operations:[['','900@lid'],['100@s.whatsapp.net','900@s.whatsapp.net'],['100@lid','900@lid'],['invalid','900@lid']]},
    {name:'hosted-discovery-miss',data:{'device-list':{'100':['99']},session:{'100_128.99':record(99)}},operations:[['100:99@hosted','900:99@hosted.lid']]},
    {name:'hosted-discovery-and-move',data:{'device-list':{'100':['99']},session:{'100.99':record(10),'100_128.99':record(99)}},operations:[['100:99@hosted','900:99@hosted.lid']]},
    {name:'invalid-hosted-destination-rollback',data:{'device-list':{'100':['0','99']},session:{'100.0':record(10),'100.99':record(99)}},operations:[['100@s.whatsapp.net','900@lid']]},
    {name:'duplicate-device-counts',data:{'device-list':{'100':['1','1']},session:{'100.1':record(11)}},operations:[['100:1@s.whatsapp.net','900@lid']]},
    {name:'failed-write-not-cached',data:{'device-list':{'100':['1']},session:{'100.1':record(11)}},failFirst:true,operations:[['100:1@s.whatsapp.net','900@lid'],['100:1@s.whatsapp.net','900@lid']]},
    {name:'cached-recreated-source',data:{'device-list':{'100':['1']},session:{'100.1':record(11)}},recreate:record(22),operations:[['100:1@s.whatsapp.net','900@lid'],['100:1@s.whatsapp.net','901@lid'],['close'],['100:1@s.whatsapp.net','901@lid']]}
  ]
  const output=[]
  for(const config of cases){let data=structuredClone(config.data),fail=config.failFirst;const keys={async get(category,ids){return Object.fromEntries(ids.filter(id=>data[category]?.[id]!==undefined).map(id=>[id,structuredClone(data[category][id])]))},async set(update){if(fail){fail=false;throw new Error('synthetic write failure')}for(const [category,values] of Object.entries(update)){data[category]??={};for(const [id,value] of Object.entries(values)){if(value===null)delete data[category][id];else data[category][id]=structuredClone(value)}}},async transaction(work){const before=structuredClone(data);try{return await work()}catch(error){data=before;throw error}}};const repo=make(keys),steps=[];for(const [index,args] of config.operations.entries()){let result,error;if(args[0]==='close')repo.close();else try{result=await repo.migrateSession(...args)}catch(e){error=e.message}steps.push({args,result:result??null,error:error??null,storage:structuredClone(data)});if(index===0&&config.recreate)data.session['100.1']=structuredClone(config.recreate)}repo.close();output.push({...config,steps})}
  await writeFile('tests/fixtures/migration.json',JSON.stringify({cases:output},null,2)+'\n')
  console.log(`Generated ${output.length} migration scenarios from upstream`)
}finally{await rm(temp,{recursive:true,force:true})}
