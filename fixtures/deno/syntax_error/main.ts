// BLD-01: syntax error on line 5 (`const x = ;`). Deploy must answer 422 with file/line/col and
// create no version. Keep the error on line 5.
export default function (): Response {
  // the next line is the syntax error
  const x = ;
  return new Response(String(x));
}
