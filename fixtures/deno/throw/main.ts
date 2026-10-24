// DNO-09: the guest throws. Expected: `500 guest_error`, the client body never contains "boom",
// the service log contains the message, a stack and the task id.
export default function (): Response {
  throw new Error("boom");
}
