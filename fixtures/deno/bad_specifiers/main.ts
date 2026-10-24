// BLD-02: three specifiers that are never allowed at deploy time. Each must be named in the 422.
// (`std:kv` is fine and must NOT be reported.)
import { kv } from "std:kv";
import passwd from "file:///etc/passwd";
import { exec } from "node:child_process";
import { nope } from "sdk:x";

export default function (): Response {
  void kv;
  void passwd;
  void exec;
  void nope;
  return new Response("unreachable");
}
