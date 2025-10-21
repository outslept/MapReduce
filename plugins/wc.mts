import type { KV, MapFn, ReduceFn } from "../src/protocol.mjs";

const WORDS = /[^A-Za-zА-Яа-я0-9_]+/g;

export const map: MapFn = (_filename, content) => {
  const out: KV[] = [];
  for (const w of content.split(WORDS)) {
    if (w.length === 0) continue; // skip empty tokens
    out.push({ key: w, value: "1" });
  }
  return out;
};

export const reduce: ReduceFn = (_key, values) => {
  let sum = 0;
  for (const v of values) sum += Number(v) | 0;
  return String(sum);
};