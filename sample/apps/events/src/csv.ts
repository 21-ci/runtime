// RFC 4180 CSV with a guard against formula injection: a cell that starts with = + - @ tab or CR
// would be evaluated by Excel / LibreOffice / Sheets, so string cells like that get a leading '.

export function csvCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  let s = typeof v === "string" ? v : String(v);
  if (typeof v === "string" && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  return /[",\r\n]|^\s|\s$/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// One record with its CRLF line ending.
export function csvLine(cells: unknown[]): string {
  return cells.map(csvCell).join(",") + "\r\n";
}

// Prepended once to a file, so Excel reads non-Latin names as UTF-8.
export const UTF8_BOM = String.fromCharCode(0xfeff);

export function toCsv(header: string[], rows: unknown[][], bom = true): string {
  return (bom ? UTF8_BOM : "") + [header, ...rows].map(csvLine).join("");
}
