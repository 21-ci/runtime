// DNO-06: three chunks, 100 ms apart. The first must reach the client long before the last.
export default function (): Response {
  const enc = new TextEncoder();
  const body = new ReadableStream({
    async start(c) {
      for (let i = 0; i < 3; i++) {
        c.enqueue(enc.encode(`chunk${i}\n`));
        await new Promise((r) => setTimeout(r, 100));
      }
      c.close();
    },
  });
  return new Response(body, { headers: { "content-type": "text/plain" } });
}
