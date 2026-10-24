// DNO-05: fetches the cloud metadata IP and a loopback port (the test passes its own listener's
// port as `?port=`). Both must reject with `egress_denied`. The error is reported, not logged,
// so any "egress" record in the service log comes from the host's guard.
async function probe(url: string): Promise<string> {
  try {
    const r = await fetch(url);
    return "resolved:" + r.status;
  } catch (e) {
    return "rejected:" + String((e as Error)?.message ?? e);
  }
}

export default async function (req: Request): Promise<Response> {
  const port = new URL(req.url).searchParams.get("port") ?? "1";
  return Response.json({
    metadata: await probe("http://169.254.169.254/latest/meta-data/"),
    control: await probe(`http://127.0.0.1:${port}/`),
  });
}
