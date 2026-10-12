import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-dispatch-'))
try {
 const source=await readFile(join(upstream,'src/Socket/messages-recv.ts'),'utf8'),jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8'),ack=await readFile(join(upstream,'src/Utils/stanza-ack.ts'),'utf8')
 const helpers=jid.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'const WAJIDDomains = '+JSON.stringify(Object.fromEntries(body.split(',').map(s=>s.trim()).filter(Boolean).map(s=>{const [key,value]=s.split('=');return [key.trim(),Number(value.trim())]}))))
 const process=source.slice(source.indexOf('const processNode = async ('),source.indexOf('\n\t// recv a message',source.indexOf('const processNode = async (')))
 const code=helpers+ack+`export async function run(node,me,ignored){const trace=[],authState={creds:{me}},NACK_REASONS={UnhandledError:500},shouldIgnoreJid=jid=>{trace.push(['filter',jid]);return ignored},sendMessageAck=async(n,code)=>trace.push(['ack',buildAckStanza(n,code,me?.id)]),offlineNodeProcessor={enqueue:(type,n)=>trace.push(['offline',type,n])},processNodeWithBuffer=async(n,id,exec)=>{trace.push(['online',id]);await exec(n)};`+process+`await processNode(node.tag,node,'handling '+node.tag,async n=>trace.push(['exec',n]));return JSON.parse(JSON.stringify(trace))}`
 await writeFile(join(temp,'dispatch.mjs'),stripTypeScriptTypes(code));const {run}=await import(pathToFileURL(join(temp,'dispatch.mjs'))),cases=[]
 for(const tag of ['message','call','receipt','notification'])for(const from of [null,'','200@s.whatsapp.net','100:3@s.whatsapp.net','100@lid','group@g.us','@s.whatsapp.net','group:bad@g.us','100:bad@s.whatsapp.net','100:70000@lid'])for(const participant of [null,'','100@s.whatsapp.net','200@s.whatsapp.net'])for(const recipient of [null,'300@lid'])for(const offline of [null,'','0','1'])for(const ignored of [false,true]) {
 const node={tag,attrs:{id:'ID',...(from===null?{}:{from}),...(participant===null?{}:{participant}),...(recipient===null?{}:{recipient}),...(offline===null?{}:{offline})}},me={id:'100:2@s.whatsapp.net',lid:'100@lid'}
 cases.push({node,me,ignored,expected:await run(node,me,ignored)})}
 await writeFile('tests/fixtures/dispatch.json',JSON.stringify({cases},null,2)+'\n');console.log(`Generated ${cases.length} complete original dispatch cases`)
}finally{await rm(temp,{recursive:true,force:true})}
