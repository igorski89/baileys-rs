// Run the pinned upstream helpers against synthetic token data.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-privacy-'))
try {
  const jid=stripTypeScriptTypes((await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8')).replace(/export enum WAJIDDomains \{[^}]*\}/,'export const WAJIDDomains = {WHATSAPP:0,LID:1,HOSTED:128,HOSTED_LID:129}'))
  await writeFile(join(temp,'jid.mjs'),jid)
  await writeFile(join(temp,'binary.mjs'),`export * from './jid.mjs';export const getBinaryNodeChild=(n,t)=>Array.isArray(n.content)?n.content.find(c=>c.tag===t):undefined;export const getBinaryNodeChildren=(n,t)=>Array.isArray(n.content)?n.content.filter(c=>c.tag===t):[];`)
  const source=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/tc-token-utils.ts'),'utf8')).replace("'../WABinary'","'./binary.mjs'")
  await writeFile(join(temp,'privacy.mjs'),source)
  const h=await import(pathToFileURL(join(temp,'privacy.mjs')))
  const now=1700000000;Date.now=()=>now*1000
  const cutoff=(Math.floor(now/604800)-3)*604800
  const timestamps=[null,0,cutoff-1,cutoff,now,String(now),' '+now+'tail','0x'+now.toString(16),'1e9','', 'invalid', '\ufeff'+now,'+1700000000','-1']
  const expiry=timestamps.map(timestamp=>({timestamp,expired:h.isTcTokenExpired(timestamp)}))
  const issue=[undefined,0,Math.floor(now/604800)*604800-1,now,now+604800].map(sender=>({sender:sender??null,issue:h.shouldSendNewTcToken(sender)}))
  const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).map(([k,x])=>[k,clean(x)])):v
  const cases=[]
  for(const [name,initial,incoming,fallback] of [
    ['new',{},[{jid:'100:2@s.whatsapp.net',t:String(now)}],''],
    ['older',{'900@lid':{token:Buffer.from([9]),timestamp:String(now),senderTimestamp:now,extra:'keep'}},[{jid:'100@s.whatsapp.net',t:String(now-1)}],''],
    ['equal',{'900@lid':{token:Buffer.from([9]),timestamp:now,senderTimestamp:now,extra:'keep'}},[{jid:'100@s.whatsapp.net',t:String(now)}],''],
    ['fallback',{},[{jid:'200@s.whatsapp.net',t:String(now)}],'100@s.whatsapp.net'],
    ['filtered',{},['0@s.whatsapp.net','13135551234@c.us','13165550012@s.whatsapp.net','123@bot','123@g.us'].map(jid=>({jid,t:String(now)})),''],
    ['numbers',{},['0','invalid','1e9','0x10','-1',String(now)].map(t=>({jid:'200@s.whatsapp.net',t})),'']
  ]){
    const data=structuredClone(initial);const stored=[]
    const keys={get:async(_,ids)=>Object.fromEntries(ids.filter(id=>data[id]).map(id=>[id,data[id]])),set:async update=>{for(const [k,v] of Object.entries(update.tctoken)){if(v===null)delete data[k];else data[k]=v}}}
    const result={tag:'iq',attrs:{},content:[{tag:'tokens',attrs:{},content:incoming.map(attrs=>({tag:'token',attrs:{type:'trusted_contact',...attrs},content:Buffer.from([1,2,3])}))}]}
    const getLIDForPN=async pn=>pn==='100@s.whatsapp.net'?'900@lid':null
    await h.storeTcTokensFromIqResult({result,fallbackJid:fallback,keys,getLIDForPN,onNewJidStored:j=>stored.push(j)})
    cases.push({name,initial:clean(initial),incoming,fallback,stored,expected:clean(data)})
  }
  await writeFile('tests/fixtures/privacy.json',JSON.stringify({now,expiry,issue,cases},null,2)+'\n')
  console.log(`Generated ${expiry.length} expiry, ${issue.length} issuance and ${cases.length} storage cases`)
} finally {await rm(temp,{recursive:true,force:true})}
