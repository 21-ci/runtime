// DNO-08: allocates (and retains) ~8 MB arrays until the heap limit is hit -> `502 oom`.
export default function (): Response {
  const keep: number[][] = [];
  while (true) {
    keep.push(new Array(1_000_000).fill(1));
  }
}
