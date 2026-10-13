// Run the MIT-licensed Baileys business socket and catalog utilities with query and upload stubs.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-business-')),hex=b=>Buffer.from(b).toString('hex')
const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):typeof v==='number'&&!Number.isFinite(v)?null:v
try {
  const boom=JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)
  for(const name of ['constants','jid-utils','encode','generic-utils']){
    let s=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8');s=s.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,b)=>'export const WAJIDDomains = {'+b.replaceAll('=',':')+'}')
    s=stripTypeScriptTypes(s).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("'@hapi/boom'",boom)
    s=s.replace("import { proto } from '../../WAProto/index.js'",'const proto = {}')
    await writeFile(join(temp,`${name}.mjs`),s)
  }
  // Catalog utilities run unchanged; only their media stream and identifier sources are stubbed.
  const utils=await readFile(join(upstream,'src/Utils/business.ts'),'utf8'),start=utils.indexOf('export const parseCatalogNode')
  await writeFile(join(temp,'utils.mjs'),`import {Boom} from ${boom};import {createHash} from 'crypto';import {createWriteStream,promises as fs} from 'fs';import {tmpdir} from 'os';import {join} from 'path';import {getBinaryNodeChild,getBinaryNodeChildren,getBinaryNodeChildString} from './generic-utils.mjs';let counter=0;const console={log(){}},generateMessageIDV2=()=>'FIXTURE'+process.pid+'-'+(counter++),getStream=async item=>({stream:(async function*(){yield item})()}),getUrlFromDirectPath=(directPath,host='mmg.whatsapp.net')=>\`https://\${host}\${directPath}\`;`+stripTypeScriptTypes(utils.slice(start)))
  const source=await readFile(join(upstream,'src/Socket/business.ts'),'utf8')
  await writeFile(join(temp,'business.mjs'),`import {createHash} from 'crypto';import {parseCatalogNode,parseCollectionsNode,parseOrderDetailsNode,parseProductNode,toProductNode,uploadingNecessaryImagesOfProduct} from './utils.mjs';import {jidNormalizedUser,S_WHATSAPP_NET} from './jid-utils.mjs';import {getBinaryNodeChild} from './generic-utils.mjs';const makeMessagesRecvSocket=c=>c.sock,getRawMediaUploadData=async(media,mediaType)=>({fileSha256:createHash('sha256').update(media).digest(),filePath:'/fixture/'+mediaType,fileLength:media.length});`+stripTypeScriptTypes(source.slice(source.indexOf('export const makeBusinessSocket'))))
  const {makeBusinessSocket}=await import(pathToFileURL(join(temp,'business.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs')))
  const n=(tag,attrs={},content)=>({tag,attrs,...(content===undefined?{}:{content})}),t=(tag,value)=>n(tag,{},Buffer.from(value)),iq=content=>n('iq',{type:'result'},content)
  const product=(id,extra={},attrs={})=>n('product',attrs,[t('id',id),n('media',{},[n('image',{},[t('request_image_url','https://media.example/requested/'+id),t('original_image_url','https://media.example/original/'+id)])]),n('status_info',{},[t('status','APPROVED'),t('can_appeal','false')]),t('name','Product '+id+' 💚'),t('retailer_id','SKU-'+id),t('url','https://wa.me/p/'+id),t('description','Description of '+id),t('price','1250000'),t('currency','EUR'),...Object.entries(extra).map(([k,v])=>t(k,v))])
  const sparse=n('product',{is_hidden:'true'},[n('media',{},[n('image',{})]),t('price','not a number')]),bare=n('product',{is_hidden:'false'})
  const catalog=iq([n('product_catalog',{},[product('100'),product('101',{},{is_hidden:'true'}),sparse,bare,n('other',{}),n('paging',{},[t('after','NEXT-CURSOR')])])])
  const collections=iq([n('collections',{},[n('collection',{},[t('id','C1'),t('name','Collection 💚'),product('100'),sparse,n('status_info',{},[t('status','APPROVED'),t('can_appeal','true')])]),n('collection',{},[n('status_info',{},[t('can_appeal','false')])]),n('collection',{}),n('other',{})])])
  const order=iq([n('order',{},[n('product',{},[t('id','100'),t('name','Ordered 💚'),n('image',{},[t('url','https://media.example/order/100')]),t('price','1250000'),t('currency','EUR'),t('quantity','2')]),n('product',{},[t('quantity','x')]),n('price',{},[t('total','2500000'),t('currency','EUR')])])])
  const image=text=>({bytes:Buffer.from(text).toString('base64')}),hosted={url:'https://mmg.whatsapp.net/v/t/existing.jpg'}
  const fields={name:'Name 💚',description:'Description\nline',retailerId:'SKU-1',price:1250000,currency:'EUR'}
  const upload={meta_hmac:'HMAC-TOKEN',fbid:1234567890123456,ts:1700000000}
  const calls=[
    ['updateBussinesProfile',[{address:'1 Example Street',email:'shop@example.com',description:'We sell things 💚',websites:['https://example.com','https://shop.example.com'],hours:{timezone:'Europe/Berlin',days:[{day:'mon',mode:'specific_hours',openTimeInMinutes:'540',closeTimeInMinutes:'1020'},{day:'sat',mode:'open_24h'},{day:'sun',mode:'appointment_only'}]}}],[iq([])]],
    ['updateBussinesProfile',[{}],[iq([])]],
    ['updateBussinesProfile',[{description:'',websites:[],hours:{timezone:'UTC',days:[]}}],[iq([n('profile',{})])]],
    ['updateBussinesProfile',[{address:null,email:'only@example.com'}],[iq([])]],
    ['updateCoverPhoto',[image('cover photo bytes')],[iq([])],upload],
    ['updateCoverPhoto',[image('second cover')],[iq([])],{fbid:42,ts:1700000001.5}],
    ['updateCoverPhoto',[image('third cover')],[iq([])],{meta_hmac:'T'}],
    ['removeCoverPhoto',['1234567890123456'],[iq([])]],
    ['getCatalog',[{}],[catalog]],
    ['getCatalog',[{jid:'555:3@s.whatsapp.net',limit:5,cursor:'CURSOR'}],[catalog]],
    ['getCatalog',[{jid:'',limit:0,cursor:''}],[iq([n('product_catalog',{},[n('paging',{})])])]],
    ['getCatalog',[{jid:'777@lid',limit:250}],[iq([n('product_catalog',{})])]],
    ['getCatalog',[{}],[iq([])]],
    ['getCollections',[],[collections]],
    ['getCollections',['555:3@s.whatsapp.net',3],[iq([n('collections',{})])]],
    ['getCollections',['',7],[iq([])]],
    ['getOrderDetails',['ORDER-1','dG9rZW4='],[order]],
    ['getOrderDetails',['ORDER-2','dG9rZW4='],[iq([n('order',{})])]],
    ['getOrderDetails',['ORDER-3','dG9rZW4='],[iq([])]],
    ['productUpdate',['100',{...fields,isHidden:true,images:[hosted,image('new image'),{url:'https://scontent.whatsapp.net/second.jpg'}]}],[iq([n('product_catalog_edit',{},[product('100')])])]],
    ['productUpdate',['100',{images:[]}],[iq([n('product_catalog_edit',{},[sparse])])]],
    ['productUpdate',['100',{name:'Renamed',isHidden:false,images:[image('one'),image('two'),image('three')]}],[iq([n('product_catalog_edit',{},[bare])])]],
    ['productUpdate',['100',{price:0,currency:'',description:'',retailerId:'',name:'',images:[hosted]}],[iq([n('product_catalog_edit',{})])]],
    ['productUpdate',['100',{images:[]}],[iq([])]],
    ['productCreate',[{...fields,originCountryCode:'IN',images:[image('created image')]}],[iq([n('product_catalog_add',{},[product('200')])])]],
    ['productCreate',[{...fields,isHidden:true,images:[hosted]}],[iq([n('product_catalog_add',{},[product('201',{},{is_hidden:'true'})])])]],
    ['productCreate',[{name:'Minimal',images:[]}],[iq([n('product_catalog_add',{},[sparse])])]],
    ['productCreate',[{...fields,images:[]}],[iq([n('other',{})])]],
    ['productDelete',[['100','101']],[iq([n('product_catalog_delete',{deleted_count:'2'})])]],
    ['productDelete',[[]],[iq([n('product_catalog_delete',{})])]],
    ['productDelete',[['100']],[iq([n('product_catalog_delete',{deleted_count:'many'})])]],
    ['productDelete',[['100']],[iq([n('product_catalog_delete',{deleted_count:''})])]],
    ['productDelete',[['100']],[iq([])]]
  ]
  // Images arrive as base64 in the fixture; upstream receives them as buffers.
  const media=v=>v&&typeof v==='object'&&'bytes' in v?Buffer.from(v.bytes,'base64'):v
  const operations=[]
  for(const [method,args,responses,uploaded] of calls){const requests=[],uploads=[],queue=[...responses]
    const query=async node=>{requests.push(node);return queue.shift()}
    const waUploadToServer=async(filePath,{mediaType,fileEncSha256B64,timeoutMs})=>{uploads.push({mediaType,hash:fileEncSha256B64,timeoutMs:timeoutMs??null});return {mediaUrl:'https://media.example/'+mediaType,directPath:'/v/t/'+Buffer.from(fileEncSha256B64,'base64').toString('hex').slice(0,12),...uploaded}}
    const sock={authState:{creds:{me:{id:'100:7@s.whatsapp.net'}}},query,waUploadToServer}
    const input=structuredClone(args).map(arg=>arg&&typeof arg==='object'&&!Array.isArray(arg)&&'images' in arg?{...arg,images:arg.images.map(media)}:media(arg))
    // A created product always states its origin, possibly as undefined.
    if(method==='productCreate')input[0]={...input[0],originCountryCode:input[0].originCountryCode}
    let expected,error=false;try{expected=await makeBusinessSocket({sock,logger:{}})[method](...input)}catch(e){error=true}
    operations.push({method,args,requests:requests.map(n=>hex(encodeBinaryNode(n))),responses:responses.map(n=>hex(encodeBinaryNode(n))),uploads:uploads.sort((a,b)=>a.hash<b.hash?-1:1),uploaded:uploaded??null,expected:clean(expected)??null,error})
  }
  await writeFile('tests/fixtures/business.json',JSON.stringify({operations},null,1)+'\n')
  console.log(`Generated ${operations.length} original business operations`)
}finally{await rm(temp,{recursive:true,force:true})}
