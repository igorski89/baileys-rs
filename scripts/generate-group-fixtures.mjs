// Run the MIT-licensed Baileys Group implementation with synthetic keys only.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import {createPrivateKey,createPublicKey} from 'node:crypto'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json'))
const temp=await mkdtemp(join(tmpdir(),'baileys-group-'))
const hex=b=>Buffer.from(b).toString('hex')
try {
  const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp,'proto.mjs'),protoSource)
  const modules=['ciphertext-message','sender-chain-key','sender-message-key','sender-key-name','sender-key-state','sender-key-record','sender-key-message','sender-key-distribution-message','group_cipher','group-session-builder','keyhelper']
  for(const name of modules) {
    let source=stripTypeScriptTypes(await readFile(join(upstream,`src/Signal/Group/${name}.ts`),'utf8'))
    source=source.replace(/from '(\.\/[^']+)'/g,(_,p)=>`from '${p}.mjs'`)
      .replace(/from 'libsignal\/src\/(crypto|curve)'/g,(_,p)=>`from ${JSON.stringify(pathToFileURL(require.resolve(`libsignal/src/${p}.js`)).href)}`)
      .replace("from '../../../WAProto/index.js'","from './proto.mjs'")
      .replace("import { BufferJSON } from '../../Utils/generics'",`const BufferJSON={reviver(k,v){if(v?.type==='Buffer'){return Buffer.from(v.data,typeof v.data==='string'?'base64':undefined)}return v}}`)
    await writeFile(join(temp,`${name}.mjs`),source)
  }
  const {SenderKeyRecord}=await import(pathToFileURL(join(temp,'sender-key-record.mjs')))
  const {SenderKeyName}=await import(pathToFileURL(join(temp,'sender-key-name.mjs')))
  const {GroupCipher}=await import(pathToFileURL(join(temp,'group_cipher.mjs')))
  const {GroupSessionBuilder}=await import(pathToFileURL(join(temp,'group-session-builder.mjs')))
  const privateKey=Buffer.alloc(32,42);privateKey[0]&=248;privateKey[31]=(privateKey[31]&127)|64
  const der=createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b656e04220420','hex'),privateKey]),format:'der',type:'pkcs8'})
  const publicKey=Buffer.concat([Buffer.from([5]),createPublicKey(der).export({format:'der',type:'spki'}).subarray(-32)])
  const sender={id:'123456789_1',deviceId:7,toString(){return `${this.id}.${this.deviceId}`}}
  const name=new SenderKeyName('synthetic@g.us',sender)
  let record=new SenderKeyRecord();record.setSenderKeyState(12345,0,Buffer.alloc(32,17),{public:publicKey,private:privateKey})
  const storage={async loadSenderKey(){return SenderKeyRecord.deserialize(Buffer.from(JSON.stringify(record.serialize())))},async storeSenderKey(n,r){record=r}}
  const initial=JSON.stringify(record.serialize())
  const builder=new GroupSessionBuilder(storage),cipher=new GroupCipher(storage,name)
  const distribution=hex((await builder.create(name)).serialize())
  const messages=[]
  for(const length of [0,1,15,16,17,32,255,256,1024]) {
    const data=Buffer.from(Array.from({length},(_,i)=>i%251)),before=hex((await builder.create(name)).serialize())
    messages.push({data:hex(data),distribution:before,ciphertext:hex(await cipher.encrypt(data)),record:JSON.stringify(record.serialize())})
  }
  const receiver=new SenderKeyRecord()
  const receiverStorage={async loadSenderKey(){return receiver},async storeSenderKey(){}}
  const {SenderKeyDistributionMessage}=await import(pathToFileURL(join(temp,'sender-key-distribution-message.mjs')))
  await new GroupSessionBuilder(receiverStorage).process(name,new SenderKeyDistributionMessage(null,null,null,null,Buffer.from(distribution,'hex')))
  const order=[8,0,4,2,7,1,6,3,5]
  for(const i of order){const data=await new GroupCipher(receiverStorage,name).decrypt(Buffer.from(messages[i].ciphertext,'hex'));if(hex(data)!==messages[i].data)throw Error('fixture decrypt mismatch')}
  const {SenderChainKey}=await import(pathToFileURL(join(temp,'sender-chain-key.mjs')))
  const {SenderKeyMessage}=await import(pathToFileURL(join(temp,'sender-key-message.mjs')))
  const crypto=require('libsignal/src/crypto.js')
  let chain=new SenderChainKey(0,Buffer.alloc(32,17))
  const gaps=[]
  for(const iteration of [2000,4001,6003]) {
    while(chain.getIteration()<iteration) chain=chain.getNext()
    const key=chain.getSenderMessageKey(),data=Buffer.from(`gap ${iteration}`)
    const ciphertext=crypto.encrypt(key.getCipherKey(),data,key.getIv())
    gaps.push({iteration,data:hex(data),ciphertext:hex(new SenderKeyMessage(12345,iteration,ciphertext,privateKey).serialize())})
  }
  const names=['synthetic@g.us','unicode-😀@g.us'].map(group=>{const n=new SenderKeyName(group,sender);return {group,user:sender.id,device:sender.deviceId,name:n.toString(),hash:n.hashCode()}})
  await writeFile('tests/fixtures/group.json',JSON.stringify({private:hex(privateKey),public:hex(publicKey),seed:hex(Buffer.alloc(32,17)),initial,distribution,messages,order,receiver:JSON.stringify(receiver.serialize()),gaps,names},null,2)+'\n')
  console.log(`Generated ${messages.length} upstream group messages, shuffled decrypt and record fixtures`)
} finally {await rm(temp,{recursive:true,force:true})}
