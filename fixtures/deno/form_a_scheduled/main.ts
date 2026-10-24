// DNO-12: a Form A service (default-exported function, no `scheduled`/`queue` export). Non-HTTP
// triggers reach it as synthetic requests:
//   interval / cron  POST /__rt/scheduled  {scheduleId, scheduledTime, payload}  + x-rt-trigger
//   queue            POST /__rt/queue      {messages:[{id, body, attempt}]}       + x-rt-trigger
// The handler echoes what it received so the test can assert on the delivered request.
export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);
  const text = await req.text();
  let body: unknown = null;
  if (text.length > 0) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { unparsable: text };
    }
  }
  return Response.json({
    method: req.method,
    path: url.pathname,
    trigger: req.headers.get("x-rt-trigger"),
    contentType: req.headers.get("content-type"),
    body,
  });
}
