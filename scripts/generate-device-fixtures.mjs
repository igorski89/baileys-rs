// Upstream device-discovery orchestration with synthetic USync results and storage.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-devices-'))
try {
  let jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8');jid=jid.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains = {'+body.replaceAll('=',':')+'}');await writeFile(join(temp,'jid.mjs'),stripTypeScriptTypes(jid))
  const signal=await readFile(join(upstream,'src/Utils/signal.ts'),'utf8'),extract=signal.slice(signal.indexOf('export const extractDeviceJids'),signal.indexOf('/**\n * get the next N keys'))
  const send=await readFile(join(upstream,'src/Socket/messages-send.ts'),'utf8'),method=send.slice(send.indexOf('const getUSyncDevices ='),send.indexOf('\n\t/**\n\t * Update Member Label'))
  const prefix=`import {jidDecode,jidNormalizedUser,jidEncode,isLidUser,isHostedLidUser,getServerFromDomainType,WAJIDDomains} from './jid.mjs';`
  const stub=`class USyncUser {withId(id){this.id=id;return this}}class USyncQuery {constructor(){this.context='interactive';this.protocols=[];this.users=[]}withContext(c){this.context=c;return this}withDeviceProtocol(){this.protocols.push('devices');return this}withLIDProtocol(){this.protocols.push('lid');return this}withUser(u){this.users.push(u.id);return this}}`
  await writeFile(join(temp,'devices.mjs'),prefix+stub+stripTypeScriptTypes(extract+`export function make(response){const logger={debug(){},trace(){},warn(){}};const cache=new Map(),storage={},queries=[],refreshes=[],mappings=[];const userDevicesCache={get:k=>cache.get(k),set:(k,v)=>cache.set(k,v)};const devicesMutex={mutex:fn=>fn()};const authState={creds:{me:{id:'100:7@s.whatsapp.net',lid:'900:7@lid'}},keys:{async set(update){Object.assign(storage,update['device-list'])}}};const signalRepository={lidMapping:{async storeLIDPNMappings(pairs){mappings.push(...pairs)}}};const assertSessions=async (lids,force)=>refreshes.push({lids,force});const sock={async executeUSyncQuery(q){queries.push({context:q.context,protocols:q.protocols,users:q.users});return response}};`+method+`return {getUSyncDevices,storage,queries,refreshes,mappings}}`))
  const {make}=await import(pathToFileURL(join(temp,'devices.mjs')))
  const response={list:[{id:'100@s.whatsapp.net',devices:{deviceList:[{id:0},{id:7,keyIndex:1},{id:8,keyIndex:2}]}},{id:'200@s.whatsapp.net',lid:'800@lid',devices:{deviceList:[{id:0},{id:1,keyIndex:3},{id:99,keyIndex:4,isHosted:true},{id:2,keyIndex:5},{id:3}]}},{id:'300@lid',devices:{deviceList:[]}}],sideList:[]}
  for(const user of response.list)for(const device of user.devices.deviceList)device.isHosted??=false
  const repo=make(response),operations=[
    {jids:['100:7@s.whatsapp.net','200:0@s.whatsapp.net','100@s.whatsapp.net','200@s.whatsapp.net'],useCache:true,ignoreZero:true},
    {jids:['200@s.whatsapp.net','100@s.whatsapp.net'],useCache:true,ignoreZero:false},
    {jids:['200@lid'],useCache:true,ignoreZero:true},
    {jids:['200@c.us','200@s.whatsapp.net'],useCache:false,ignoreZero:false},
    {jids:['300@lid'],useCache:true,ignoreZero:false},
    {jids:['300@lid'],useCache:true,ignoreZero:false},
    {jids:['200:0@lid','100:7@s.whatsapp.net'],useCache:true,ignoreZero:true},
    {jids:[],useCache:true,ignoreZero:false}
  ],steps=[]
  for(const operation of operations){const previous=repo.queries.length;const result=await repo.getUSyncDevices(operation.jids,operation.useCache,operation.ignoreZero);steps.push({operation,result,toFetch:repo.queries[previous]?.users??[],storage:structuredClone(repo.storage),refreshes:structuredClone(repo.refreshes),mappings:structuredClone(repo.mappings)})}
  const assertion=send.slice(send.indexOf('const assertSessions ='),send.indexOf('const sendPeerDataOperationMessage ='))
  await writeFile(join(temp,'assert.mjs'),`import {isPnUser,isHostedPnUser,isLidUser,isHostedLidUser} from './jid.mjs';`+stripTypeScriptTypes(`export function make(config){const logger={debug(){}};const S_WHATSAPP_NET='@s.whatsapp.net';const queries=[],validated=[],mapped=[];const query=async node=>{queries.push(node);return {}};const parseAndInjectE2ESessions=async()=>{};const signalRepository={async validateSession(jid){validated.push(jid);return {exists:config.valid.includes(jid)}},lidMapping:{async getLIDsForPNs(jids){mapped.push(jids);return jids.flatMap(pn=>config.maps[pn]?[{pn,lid:config.maps[pn]}]:[])}}};`+assertion+`return {assertSessions,queries,validated,mapped}}`))
  const {make:makeAssert}=await import(pathToFileURL(join(temp,'assert.mjs')))
  const assertions=[]
  for(const config of [{jids:[],force:false,valid:[],maps:{}},{jids:['100@s.whatsapp.net','100@s.whatsapp.net'],force:false,valid:['100@s.whatsapp.net'],maps:{}},{jids:['200:1@s.whatsapp.net','800@lid','200:1@s.whatsapp.net','400@s.whatsapp.net'],force:false,valid:[],maps:{'200:1@s.whatsapp.net':'900:1@lid'}},{jids:['200@s.whatsapp.net','800:7@lid','800:7@lid'],force:true,valid:['200@s.whatsapp.net'],maps:{'200@s.whatsapp.net':'900@lid'}},{jids:['400@s.whatsapp.net'],force:true,valid:[],maps:{}}]){const repo=makeAssert(config);const fetched=await repo.assertSessions(config.jids,config.force);assertions.push({config,fetched,queries:repo.queries,validated:repo.validated,mapped:repo.mapped})}
  await writeFile('tests/fixtures/devices.json',JSON.stringify({response,me:'100:7@s.whatsapp.net',lid:'900:7@lid',steps,assertions},null,2)+'\n')
  console.log(`Generated ${steps.length} device-discovery operations from upstream`)
}finally{await rm(temp,{recursive:true,force:true})}
