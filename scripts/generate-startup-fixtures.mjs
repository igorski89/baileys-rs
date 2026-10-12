// Run complete pinned property/blocklist helpers and original node dictionary helpers.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-startup-'))
try {
 const source=await readFile(join(upstream,'src/Socket/chats.ts'),'utf8'),binary=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8')
 const helpers=binary.slice(binary.indexOf('const indexCache'),binary.indexOf('export const getBinaryNodeChildBuffer'))+binary.slice(binary.indexOf('export const reduceBinaryNodeToDictionary'),binary.indexOf('export const getBinaryNodeMessages'))
 const props=source.slice(source.indexOf('const fetchProps ='),source.indexOf('\n\t/**',source.indexOf('const fetchProps =')))
 const block=source.slice(source.indexOf('const fetchBlocklist ='),source.indexOf('const updateBlockStatus ='))
 const factory=`export async function run(kind,response,lastHash){const requests=[],events=[],authState={creds:{...(lastHash===null?{}:{lastPropHash:lastHash})}},serverProps={privacyTokenOn1to1:true,profilePicPrivacyToken:true,lidTrustedTokenIssueToLid:false},S_WHATSAPP_NET='@s.whatsapp.net',logger={debug(){}},ev={emit:(name,data)=>events.push({name,data:JSON.parse(JSON.stringify(data))})},query=async node=>{requests.push(node);return response};`
 await writeFile(join(temp,'startup.mjs'),stripTypeScriptTypes(helpers+factory+props+block+`const result=await (kind==='props'?fetchProps():fetchBlocklist());return {requests,events,result:JSON.parse(JSON.stringify(result)),serverProps,hash:authState.creds.lastPropHash??null};}`))
 const {run}=await import(pathToFileURL(join(temp,'startup.mjs'))),node=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),prop=(attrs)=>node('prop',attrs),cases=[]
 const fields=[[],[prop({name:'10518',value:'false'}),prop({name:'9666',value:'1'}),prop({name:'14303',value:'true'})],[prop({name:'privacy_token_sending_on_all_1_on_1_messages',value:'false'}),prop({name:'10518',value:'true'})],[prop({config_code:'10518',config_value:'0'}),prop({name:'9666',value:'',config_value:'true'})],[prop({name:'10518',value:'false'}),prop({name:'10518'})],[prop({name:'',value:'x'}),prop({value:'undefined-name'}),node('ignored',{name:'10518',value:'false'})]]
 for(const lastHash of [null,'','old'])for(const hash of [null,'','new','old'])for(const list of fields)for(const leading of [false,true]) {
  const response=node('iq',{},[...(leading?[node('ignored')]:[]),node('props',hash===null?{}:{hash},list)]);cases.push({kind:'props',lastHash,response,expected:await run('props',response,lastHash)})
 }
 const blockResponses=[node('iq'),node('iq',{},[node('other')]),node('iq',{},[node('list',{},[node('item',{jid:'200@s.whatsapp.net'}),node('item',{}),node('item',{jid:''}),node('item',{jid:'200@s.whatsapp.net'}),node('other',{jid:'ignored'})])])]
 for(const response of blockResponses) {cases.push({kind:'blocklist',lastHash:null,response,expected:await run('blocklist',response,null)})}
 cases.push({kind:'props',lastHash:'old',response:node('iq'),expected:await run('props',node('iq'),'old')})
 await writeFile('tests/fixtures/startup.json',JSON.stringify({cases},null,2)+'\n');console.log(`Generated ${cases.length} complete original startup-query cases`)
}finally{await rm(temp,{recursive:true,force:true})}
