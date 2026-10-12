// Load the pinned original buffer with its actual message helpers.
import {stripTypeScriptTypes} from 'node:module'
import {readFile,writeFile} from 'node:fs/promises'
import {join} from 'node:path'
import {pathToFileURL} from 'node:url'
export async function writeEventBuffer(upstream,temp) {
 const source=await readFile(join(upstream,'src/Utils/event-buffer.ts'),'utf8'),messages=await readFile(join(upstream,'src/Utils/messages.ts'),'utf8'),process=await readFile(join(upstream,'src/Utils/process-message.ts'),'utf8'),generics=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8');
 const helpers=messages.slice(messages.indexOf('export const getContentType'),messages.indexOf('export const extractMessageContent'))+messages.slice(messages.indexOf('export const updateMessageWithReceipt'),messages.indexOf('export const updateMessageWithPollUpdate'))+generics.slice(generics.indexOf('export const getKeyAuthor'),generics.indexOf('export const isStringNullOrEmpty'))+generics.slice(generics.indexOf('export function trimUndefined'),generics.indexOf('const CROCKFORD_CHARACTERS'))+process.slice(process.indexOf('const REAL_MSG_STUB_TYPES'),process.indexOf('async function storeTcTokensFromHistorySync'))+process.slice(process.indexOf('export const isRealMessage'),process.indexOf('export const getChatId'));
 const buffer=stripTypeScriptTypes(source).replace(/import \{[^}]*\} from '\.\.\/Types'\s*/g,'').replace(/import \{[^}]*\} from '\.\/generics'\s*/g,'').replace(/import \{[^}]*\} from '\.\/messages'\s*/g,'').replace(/import \{[^}]*\} from '\.\/process-message'\s*/g,'');
 await writeFile(join(temp,'event-buffer.mjs'),`import {proto} from './proto.mjs';const WAMessageStatus=proto.WebMessageInfo.Status,WAMessageStubType=proto.WebMessageInfo.StubType;`+stripTypeScriptTypes(helpers)+buffer);
 return import(pathToFileURL(join(temp,'event-buffer.mjs')))
}
