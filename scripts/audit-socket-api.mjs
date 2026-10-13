// List every method the upstream Baileys socket layers return and whether the Rust port has a function of the same name.
// Usage: node scripts/audit-socket-api.mjs ../Baileys   (exit code 1 when methods are unaccounted for)
import {readFileSync,readdirSync,statSync} from 'node:fs'
import {join,resolve} from 'node:path'
const upstream=resolve(process.argv[2]??'../Baileys')
const walk=dir=>readdirSync(dir).flatMap(name=>{const path=join(dir,name);return statSync(path).isDirectory()?walk(path):path.endsWith('.rs')?[path]:[]})
const rust=walk('src').map(path=>readFileSync(path,'utf8')).join('\n'),functions=new Set([...rust.matchAll(/\bfn\s+([a-z0-9_]+)/g)].map(m=>m[1]))
const snake=name=>name.replace(/([a-z0-9])([A-Z])/g,'$1_$2').replace(/([A-Z]+)([A-Z][a-z])/g,'$1_$2').toLowerCase()
// Upstream names whose Rust counterpart is named differently, and members that are not operations.
const renamed={onWhatsApp:'on_whatsapp',refreshMediaConn:'refresh_media_connection',getMediaHost:'media_host',waUploadToServer:'upload_media',updateServerTimeOffset:'update_server_time',sendUnifiedSession:'unified_session',
  // Start-up pre-key check and connection-update waiting are internal to the actor or served by the event stream.
  uploadPreKeysToServerIfRequired:null,waitForConnectionUpdate:null,placeholderResendCache:null,userDevicesCache:null,devicesMutex:null,
  updateBussinesProfile:'update_business_profile',sendWAMBuffer:'send_wam_buffer',executeUSyncQuery:'execute_usync',getUSyncDevices:'get_usync_devices',onUnexpectedError:null,ws:null,ev:null,type:null,authState:null,signalRepository:null,user:null,logger:null,
  waitForSocketOpen:'wait_for_open',end:'close',requestPairingCode:'request_pairing_code',generateMessageTag:null,waitForMessage:null,sendRawMessage:null,registerSocketEndHandler:null,wamBuffer:null,messageMutex:null,receiptMutex:null,appStatePatchMutex:null,notificationMutex:null,processingMutex:null,createBufferedFunction:null}
const report=[]
for(const file of readdirSync(join(upstream,'src/Socket')).filter(f=>f.endsWith('.ts')&&f!=='index.ts')){
  const source=readFileSync(join(upstream,'src/Socket',file),'utf8'),start=source.lastIndexOf('\n\treturn {')
  if(start<0)continue
  let depth=0,end=start;for(let i=source.indexOf('{',start);i<source.length;i++){if(source[i]==='{')depth++;if(source[i]==='}'&&--depth===0){end=i;break}}
  // Top-level members of the returned object: `name,`, `name: value` and `name(args) {` at one tab deeper than the return.
  const names=[...source.slice(start,end).matchAll(/^\t\t([A-Za-z_][A-Za-z0-9_]*)\s*(?:,|:|\(|$)/gm)].map(m=>m[1])
  for(const name of new Set(names)){
    const target=name in renamed?renamed[name]:snake(name)
    report.push({file,name,target,status:target===null?'not an operation':functions.has(target)?'covered':'MISSING'})
  }
}
const missing=report.filter(r=>r.status==='MISSING')
for(const r of report)console.log(`${r.status.padEnd(16)} ${r.file.padEnd(20)} ${r.name}${r.target&&r.target!==snake(r.name)?' -> '+r.target:''}`)
console.log(`\n${report.filter(r=>r.status==='covered').length} covered, ${missing.length} missing, ${report.filter(r=>r.status==='not an operation').length} not operations`)
process.exit(missing.length?1:0)
