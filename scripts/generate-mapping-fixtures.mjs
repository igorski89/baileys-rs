// Synthetic PN/LID fixtures from the original MIT-licensed Baileys store.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json'))
const temp=await mkdtemp(join(tmpdir(),'baileys-mapping-'))
try {
  let jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8')
  jid=jid.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains = {'+body.replaceAll('=',':')+'}')
  await writeFile(join(temp,'jid.mjs'),stripTypeScriptTypes(jid))
  const source=stripTypeScriptTypes(await readFile(join(upstream,'src/Signal/lid-mapping.ts'),'utf8'))
    .replace("from 'lru-cache'",`from ${JSON.stringify(pathToFileURL(require.resolve('lru-cache')).href)}`)
    .replace("from '../WABinary'","from './jid.mjs'")
  await writeFile(join(temp,'mapping.mjs'),source)
  const {LIDMappingStore}=await import(pathToFileURL(join(temp,'mapping.mjs')))
  let data={},calls=[]
  const keys={async get(category,ids){return Object.fromEntries(ids.filter(id=>id in data).map(id=>[id,data[id]]))},async set(update){Object.assign(data,update['lid-mapping'])},async transaction(work){await work()}}
  const logger={warn(){},trace(){},debug(){}}
  const resolver=async pns=>{calls.push(pns);return pns.flatMap(pn=>pn==='300@s.whatsapp.net'?[{pn,lid:'700@lid'}]:pn==='400@s.whatsapp.net'?[{pn,lid:'600@lid'}]:[])}
  const store=new LIDMappingStore(keys,logger,resolver)
  const operations=[
    ['storeLIDPNMappings',[{pn:'100@s.whatsapp.net',lid:'900@lid'},{pn:'200:7@s.whatsapp.net',lid:'800:7@lid'}]],
    ['getLIDsForPNs',['200:7@s.whatsapp.net','100:0@s.whatsapp.net','200:7@s.whatsapp.net','100:99@hosted','bad@lid']],
    ['getPNsForLIDs',['900@lid','800:7@lid','900:99@hosted.lid','bad@s.whatsapp.net']],
    ['storeLIDPNMappings',[{pn:'100@s.whatsapp.net',lid:'901@lid'}]],
    ['getPNsForLIDs',['900:7@lid','901@lid']],
    ['storeLIDPNMappings',[{pn:'100@s.whatsapp.net',lid:'902@lid'},{pn:'100@s.whatsapp.net',lid:'901@lid'}]],
    ['getLIDForPN','100:7@s.whatsapp.net'],
    ['storeLIDPNMappings',[{pn:'999@lid',lid:'111@s.whatsapp.net'},{pn:'222:99@hosted',lid:'333:99@hosted.lid'},{pn:'invalid',lid:'bad'}]],
    ['getLIDsForPNs',['999@s.whatsapp.net','222:99@hosted']],
    ['getLIDsForPNs',['300:7@s.whatsapp.net','400:99@hosted','300:0@s.whatsapp.net','300:7@s.whatsapp.net']],
    ['getLIDsForPNs',['300:0@s.whatsapp.net','400:99@hosted']],
    ['getPNForLID','700@lid'],
    ['getLIDsForPNs',[]],['getPNsForLIDs',[]],['getLIDForPN','unknown@s.whatsapp.net']
  ]
  const steps=[]
  for(const [method,input] of operations) {const result=await store[method](input);steps.push({method,input,result:result??null,storage:{...data},calls:structuredClone(calls)})}
  store.close()
  steps.push({method:'close',input:null,result:null,storage:{...data},calls:structuredClone(calls)})
  for(const [method,input] of [['getLIDsForPNs',['100:7@s.whatsapp.net','300@s.whatsapp.net']],['getPNsForLIDs',['700:7@lid','900@lid']]]) {
    steps.push({method,input,result:await store[method](input),storage:{...data},calls:structuredClone(calls)})
  }
  store.close()
  await writeFile('tests/fixtures/mapping.json',JSON.stringify({steps},null,2)+'\n')
  console.log(`Generated ${steps.length} mapping operations from upstream`)
} finally {await rm(temp,{recursive:true,force:true})}
