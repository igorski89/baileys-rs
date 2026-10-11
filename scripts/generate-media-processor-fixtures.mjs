// Execute upstream media processors on synthetic images and audio. Requires
// .cache/fixture-tools dependencies sharp, music-metadata, audio-decode and FFmpeg.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {execFileSync} from 'node:child_process'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json'))
const temp=await mkdtemp(join(tmpdir(),'baileys-process-')),hex=b=>Buffer.from(b).toString('hex')
try {
 const source=await readFile(join(upstream,'src/Utils/messages-media.ts'),'utf8')
 const image=source.slice(source.indexOf('const extractVideoThumb'),source.indexOf('export const encodeBase64EncodedStringForUpload'))
 const profile=source.slice(source.indexOf('export const generateProfilePicture'),source.indexOf('/** gets the SHA256'))
 const audio=source.slice(source.indexOf('export async function getAudioDuration'),source.indexOf('export const toReadable'))
 const thumbnail=source.slice(source.indexOf('export async function generateThumbnail'),source.indexOf('export const getHttpStream'))
 const prefix=`import {exec} from 'node:child_process';import {Readable} from 'node:stream';import {createReadStream,promises as fs} from 'node:fs';import {join} from 'node:path';import {tmpdir} from 'node:os';import sharp from ${JSON.stringify(pathToFileURL(require.resolve('sharp')).href)};const getImageProcessingLibrary=async()=>({sharp:{default:sharp}}),getTmpFilesDirectory=tmpdir,generateMessageIDV2=()=>Math.random().toString(16).slice(2),toBuffer=async stream=>{const chunks=[];for await(const c of stream)chunks.push(c);return Buffer.concat(chunks)},getStream=async value=>({stream:Readable.from(value)});class Boom extends Error {};`
 const body=stripTypeScriptTypes(image+profile+audio+thumbnail).replaceAll("import('music-metadata')",`import(${JSON.stringify(pathToFileURL(require.resolve('music-metadata')).href)})`).replaceAll("import('audio-decode')",`import(${JSON.stringify(pathToFileURL(require.resolve('audio-decode')).href)})`)
 await writeFile(join(temp,'processors.mjs'),prefix+body)
 const helpers=await import(pathToFileURL(join(temp,'processors.mjs'))),sharp=(await import(pathToFileURL(require.resolve('sharp')))).default
 const images=[]
 for(const [width,height] of [[16,16],[64,32],[32,64],[65,17],[1,64]]) {
  const rgb=Buffer.alloc(width*height*3);for(let y=0;y<height;y++)for(let x=0;x<width;x++){const i=(y*width+x)*3;rgb[i]=x<width/3?240:20;rgb[i+1]=x>=width/3&&x<2*width/3?240:20;rgb[i+2]=x>=2*width/3?240:20}
  const input=await sharp(rgb,{raw:{width,height,channels:3}}).png().toBuffer(),thumb=await helpers.extractImageThumb(input),metadata=await sharp(thumb.buffer).metadata()
  const profile=await helpers.generateProfilePicture(input,{width:40,height:20}),profileMetadata=await sharp(profile.img).metadata()
  images.push({width,height,input:hex(input),thumbnail:{width:metadata.width,height:metadata.height,original:thumb.original},profile:{width:profileMetadata.width,height:profileMetadata.height}})
 }
 function wav(count,channels,bits,float,mode){const size=bits/8,data=Buffer.alloc(count*channels*size),buffer=Buffer.alloc(44+data.length),rate=8000
  buffer.write('RIFF');buffer.writeUInt32LE(buffer.length-8,4);buffer.write('WAVEfmt ',8);buffer.writeUInt32LE(16,16);buffer.writeUInt16LE(float?3:1,20);buffer.writeUInt16LE(channels,22);buffer.writeUInt32LE(rate,24);buffer.writeUInt32LE(rate*channels*size,28);buffer.writeUInt16LE(channels*size,32);buffer.writeUInt16LE(bits,34);buffer.write('data',36);buffer.writeUInt32LE(data.length,40)
  for(let i=0;i<count;i++)for(let c=0;c<channels;c++){let v=mode==='silence'?0:mode==='constant'?.5:mode==='extrema'?[-1,0,1][i%3]:Math.sin(i*.13)*(1+i%83)/84;if(c>0)v=.9;const p=(i*channels+c)*size;if(float)data.writeFloatLE(v,p);else if(bits===8)data.writeUInt8(Math.round((v+1)*127.5),p);else if(bits===16)data.writeInt16LE(mode==='extrema'&&v===-1?-32768:Math.round(v*32767),p);else if(bits===32)data.writeInt32LE(mode==='extrema'&&v===-1?-2147483648:Math.round(v*2147483647),p);else data.writeIntLE(mode==='extrema'&&v===-1?-8388608:Math.round(v*8388607),p,3)}data.copy(buffer,44);return buffer}
 const audios=[]
 for(const count of [1,63,64,65,128,133,8011])for(const [channels,bits,float] of [[1,8,false],[1,16,false],[2,16,false],[1,24,false],[2,32,true],[1,32,false]])for(const mode of ['varying','silence','constant','extrema']) {
  const input=wav(count,channels,bits,float,mode),wave=await helpers.getAudioWaveform(input),duration=await helpers.getAudioDuration(input)
  audios.push({count,channels,bits,float,mode,input:hex(input),duration,waveform:wave?hex(wave):null})
 }
 const raw=wav(8011,2,16,false,'varying'),path=join(temp,'audio.wav');await writeFile(path,raw)
 const compressed=[]
 // audio-decode's FLAC loader overwrites its MP3 cache entry; isolate MP3 first.
 // Nonpooled buffers avoid its buffer-offset bug. Both partial/full final FLAC frames are covered.
 for(const [format,codec,flacCount=8011] of [['mp3','libmp3lame'],['flac','flac'],['flac','flac',7680],['ogg','vorbis'],['opus','libopus']]) {
  const sourcePath=format==='flac'?join(temp,'flac-input.wav'):path;if(format==='flac')await writeFile(sourcePath,wav(flacCount,2,16,false,'varying'));
  const output=join(temp,'audio.'+format);execFileSync('ffmpeg',['-v','error','-i',sourcePath,'-c:a',codec,'-strict','-2','-y',output]);const read=await readFile(output),input=Buffer.alloc(read.length);read.copy(input);const wave=await helpers.getAudioWaveform(input),duration=await helpers.getAudioDuration(input)
  compressed.push({format,input:hex(input),duration,waveform:wave?hex(wave):null})
 }
 const videoPath=join(temp,'video.mp4');execFileSync('ffmpeg',['-v','error','-f','lavfi','-i','color=red:s=64x32:r=1','-t','1','-c:v','mpeg4','-y',videoPath]);const video=await helpers.generateThumbnail(videoPath,'video',{});
 await writeFile('tests/fixtures/media_processors.json',JSON.stringify({images,audios,compressed,video:{input:hex(await readFile(videoPath)),jpeg:hex(Buffer.from(video.thumbnail,'base64'))}},null,2)+'\n')
 console.log(`Generated ${images.length} image, ${audios.length} PCM, ${compressed.length} compressed-audio and one video processor fixtures`)
}finally {await rm(temp,{recursive:true,force:true})}
