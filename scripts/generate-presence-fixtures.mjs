import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-presence-'))
try {
 const source=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8')
 const sending=source.slice(source.indexOf('const sendPresenceUpdate ='),source.indexOf('\n\t/**',source.indexOf('const sendPresenceUpdate =')))
 const subscribing=source.slice(source.indexOf('const presenceSubscribe ='),source.indexOf('const handlePresenceUpdate ='))
 const receiving=source.slice(source.indexOf('const handlePresenceUpdate ='),source.indexOf('\n\tconst appPatch ='))
 const jidSource=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8')
 const jidDecode=jidSource.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'const WAJIDDomains = '+JSON.stringify(Object.fromEntries(body.split(',').map(s=>s.trim()).filter(Boolean).map(s=>{const [key,value]=s.split('=');return [key.trim(),Number(value.trim())]}))))
 const module=jidDecode+`export async function run(kind,input,ignored=false){const events=[],nodes=[],lookups=[],authState={creds:{me:input.me}},S_WHATSAPP_NET='@s.whatsapp.net',logger={warn(){},error(){}},ev={emit:(name,data)=>events.push({name,data})},shouldIgnoreJid=()=>ignored,sendUnifiedSession=async()=>nodes.push({tag:'unified_session'}),sendNode=async n=>nodes.push(n),generateMessageTag=()=> 'test-id',getLIDForPN=()=>undefined,buildTcTokenFromJid=async ({jid})=>{lookups.push(jid);return input.content};`+sending+subscribing+receiving+`try {if(kind==='send')await sendPresenceUpdate(input.type,input.to);else if(kind==='subscribe')await presenceSubscribe(input.to);else handlePresenceUpdate(input);return JSON.parse(JSON.stringify(kind==='subscribe'?{nodes,lookups}:{nodes,events}));}catch(error){if(!(error instanceof TypeError))throw error;return {error:true}}}`
 await writeFile(join(temp,'presence.mjs'),stripTypeScriptTypes(module))
 const {run}=await import(pathToFileURL(join(temp,'presence.mjs'))),cases=[]
 for(const name of [null,'','Alice','@A@@B@'])for(const type of ['available','unavailable','composing','recording','paused'])for(const to of [null,'200@s.whatsapp.net','200:3@lid','200@hosted.lid'])for(const lid of [null,'100@lid']) {
 const input={me:{id:'100:2@s.whatsapp.net',...(name===null?{}:{name}),...(lid===null?{}:{lid})},type,...(to===null?{}:{to})}
 // Source allows an undefined from that cannot be encoded by a native binary node.
 if(!['available','unavailable'].includes(type)&&to?.endsWith('@lid')&&lid===null)continue
 cases.push({kind:'send',input,expected:await run('send',input)})
 }
 for(const tag of ['presence','chatstate'])for(const from of ['200@s.whatsapp.net','group@g.us','@s.whatsapp.net'])for(const ignored of [false,true])for(const participant of [null,'','300@lid']) {
 const attrs={from,...(participant===null?{}:{participant})}
 const variations=tag==='presence'?[{}, {type:'unavailable',last:'deny',count:''},{type:'composing',last:'0',count:'0'},{last:'0x10',count:'3.5'},{last:'invalid',count:'Infinity'},{last:' ',count:'-3'}].map(extra=>({tag,attrs:{...attrs,...extra}})):[undefined,[],[{tag:'paused',attrs:{}}],[{tag:'paused',attrs:{media:'audio'}}],[{tag:'composing',attrs:{}}],[{tag:'future',attrs:{}}]].map(content=>({tag,attrs,...(content===undefined?{}:{content})}))
 for(const input of variations)cases.push({kind:'receive',input,ignored,expected:await run('receive',input,ignored)})
 }
 const subscriptions=[];for(const to of ['200:3@c.us','200:3@s.whatsapp.net','900:5@lid','200@hosted','200@hosted.lid','group@g.us','channel@newsletter','status@broadcast','200@bot','invalid'])for(const content of [undefined,[{tag:'tctoken',attrs:{t:'123'}}]]){const input={to,...(content===undefined?{}:{content})};subscriptions.push({input,expected:await run('subscribe',input)})}
 await writeFile('tests/fixtures/presence.json',JSON.stringify({cases,subscriptions},null,2)+'\n');console.log(`Generated ${cases.length} complete original presence cases and ${subscriptions.length} subscriptions`)
}finally{await rm(temp,{recursive:true,force:true})}
