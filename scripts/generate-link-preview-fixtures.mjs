// Run the MIT-licensed Baileys getUrlInfo with its link-preview-js dependency against synthetic pages served by a fetch stub.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-link-preview-'))
const clean=v=>Buffer.isBuffer(v)||v instanceof Uint8Array?{type:'Buffer',data:Buffer.from(v).toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):v
try {
  const source=await readFile(join(upstream,'src/Utils/link-preview.ts'),'utf8')
  // Image handling is stubbed: thumbnails and uploads report what they were asked for.
  const header=`const getHttpStream=async url=>{if(url.includes('unreachable'))throw Error('image fetch failed');return url},extractImageThumb=async(stream,width)=>({buffer:Buffer.from('thumbnail:'+stream+':'+width)}),prepareWAMessageMedia=async(message,options)=>{if(!message.image.url)throw new TypeError('missing image');if(options.mediaTypeOverride!=='thumbnail-link')throw Error('unexpected media type');return message.image.url.includes('plain')?{}:{imageMessage:{url:'https://mmg.whatsapp.net/hq',directPath:'/hq',jpegThumbnail:message.image.url.includes('nothumb')?undefined:Buffer.from('hq:'+message.image.url),width:640,height:480}}};`
  await writeFile(join(temp,'link-preview.mjs'),header+stripTypeScriptTypes(source.slice(source.indexOf('const THUMBNAIL_WIDTH_PX'))).replace("import('link-preview-js')",`import(${JSON.stringify(pathToFileURL(require.resolve('link-preview-js')).href)})`))
  const {getUrlInfo}=await import(pathToFileURL(join(temp,'link-preview.mjs')))
  const html=(head,body='')=>`<!doctype html><html><head>${head}</head><body>${body}</body></html>`
  const og=html(`<title>Fallback title</title><meta property="og:title" content="Open Graph &amp; title 💚"><meta name="description" content="Plain description"><meta property="og:description" content="OG description"><meta property="og:image" content="/images/cover.jpg"><meta property="og:image" content="https://cdn.example.com/second.png">`)
  const page=(body,type='text/html; charset=utf-8',url)=>({body,type,url})
  const pages={
    'https://example.com/og':page(og),
    'https://example.com/title-only':page(html('<title> Title &lt;only&gt; </title>')),
    'https://example.com/name-title':page(html('<title>Fallback</title><meta name="og:title" content="Named title"><meta name="Description" content="Capital description"><meta name="og:image" content="named.jpg">')),
    'https://example.com/empty-og':page(html('<title>Used title</title><meta property="og:title" content=""><meta name="description" content=""><meta property="og:description" content="Fallback description"><meta property="og:image" content=""><link rel="image_src" href="../rel.png">')),
    'https://example.com/first-meta-wins':page(html('<meta property="og:title"><meta property="og:title" content="Second title"><title>Fallback</title><meta name="description"><meta name="description" content="Second description">')),
    'https://example.com/no-title':page(html('<meta name="description" content="No title here"><meta property="og:image" content="/x.jpg">')),
    'https://example.com/body-title':page(html('','<title>Body title</title><p>text</p>')),
    'https://example.com/two-titles':page('<html><head><title>One</title><title>Two</title></head></html>'),
    'https://example.com/a/b/imgs':page(html('<title>Images</title>','<img src="one.png"><img><img src="/two.png"><img src="one.png"><img src="//cdn.example.com/three.png"><img src="https://other.example/four.png?a=1&amp;b=2">')),
    'https://example.com/rel-case':page(html('<title>Rel</title><link rel="IMAGE_SRC" href="upper.png">','<img src="fallback.png">')),
    'https://example.com/attr-case':page(html('<TITLE>Upper tags</TITLE><META PROPERTY="og:image" CONTENT="upper-meta.png"><meta property="OG:TITLE" content="Ignored upper value">')),
    'https://example.com/no-description':page(html('<title>No description</title><meta property="og:image" content="https://cdn.example.com/unreachable.jpg">')),
    'https://example.com/empty-description':page(html('<title>Empty description</title><meta property="og:description" content="">')),
    'https://example.com/plain-hq':page(html('<title>Plain upload</title><meta property="og:image" content="https://cdn.example.com/plain.jpg">')),
    'https://example.com/nothumb-hq':page(html('<title>No thumb upload</title><meta property="og:image" content="https://cdn.example.com/nothumb.jpg">')),
    'https://example.com/script':page(html('<script>var t="<title>Script title</title>"</script><title>Real title</title><!-- <meta property="og:title" content="Commented"> -->')),
    'https://example.com/text':page('Just plain text with <title>Inline title</title>','text/plain'),
    'https://example.com/unknown-type':page(html('<title>Unknown type</title>'),'x-custom/thing'),
    'https://example.com/no-type':page(html('<title>No content type</title>'),null),
    'https://example.com/image.png':page('binary','image/png'),
    'https://example.com/video':page(html('<title>Video</title>'),'video/mp4'),
    'https://example.com/audio':page(html('<title>Audio</title>'),'audio/mpeg; codecs=x'),
    'https://example.com/json':page('{"title":"x"}','application/json'),
    'https://example.com/xhtml':page(html('<title>XHTML</title>'),'application/xhtml+xml'),
    'https://example.com/redirect':page(html('<title>Redirected</title><meta property="og:image" content="img.png">'),'text/html','https://www.example.com/final/page'),
    'https://example.com/':page(html('<title>Root</title>')),
    'https://example.com/fragment#section':page(html('<title>Fragment</title>')),
    'https://sub.example.co.uk:8443/path?q=1':page(html('<title>Port and query</title>')),
    'http://example.com/insecure':page(html('<title>Insecure</title>')),
    'https://xn--bcher-kva.example/':page(html('<title>Punycode</title>')),
    'https://8.8.8.8/dns':page(html('<title>Public address</title>'))
  }
  const calls=[]
  globalThis.fetch=async(url,options)=>{calls.push({url,redirect:options.redirect});const found=pages[url];if(!found)throw new TypeError('fetch failed');return {status:200,url:found.url??url,headers:new Headers(found.type?{'content-type':found.type}:{}),text:async()=>found.body}}
  const texts=[...Object.keys(pages),'example.com/og','www.example.com','https://missing.example/page','https://10.0.0.1/private','https://127.0.0.1/','https://192.168.1.1/x','https://172.16.0.1/','https://169.254.1.1/','https://224.0.0.1/','https://0.0.0.0/','https://1.2.3.255/','https://300.1.2.3/','https://localhost/x','https://localhost:8080','https://example/x','https://example.c/x','https://user:pass@example.com/og','ftp://example.com/og','HTTPS://EXAMPLE.COM/og','https://example.com/og extra words','first https://example.com/title-only second','line\nhttps://example.com/title-only','https://exa_mple.com/','https://example.com:1/','https://example.com:123456/','https://-bad-.example.com/','https://example.com/pa th','https://example.com./','https://bücher.example/','','   ','not a url']
  const cases=[]
  for(const upload of [false,true])for(const text of texts){
    calls.length=0;let expected,error
    try{expected=await getUrlInfo(text,{thumbnailWidth:192,fetchOpts:{timeout:3000},uploadImage:upload?async()=>({}):undefined,logger:{debug(){}}})}catch(e){error=e.message}
    cases.push({text,upload,fetched:[...calls],expected:clean(expected)??null,error:error??null})
  }
  await writeFile('tests/fixtures/link-preview.json',JSON.stringify({pages,cases},null,1)+'\n')
  console.log(`Generated ${cases.length} original link preview cases over ${Object.keys(pages).length} pages`)
}finally{await rm(temp,{recursive:true,force:true})}
