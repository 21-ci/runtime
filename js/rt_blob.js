// `std:blob` -- streamed object storage: keys up to 1 KB, objects up to the plan's blob cap.
//
//   get(key)                 -> { body: ReadableStream, size, contentType } | null
//   head(key)                -> { size, contentType } | null
//   put(key, body, {contentType}) -> void       body streams in; no full buffering
//   delete(key)              -> boolean
//   list({prefix, cursor, limit}) -> { objects: [{ key, size, contentType }], cursor: string | null }
export const blob = globalThis.__rt.blob;
