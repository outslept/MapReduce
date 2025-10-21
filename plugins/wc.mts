import type { KV, MapFn, ReduceFn } from "../src/protocol.mjs";

const WORDS = /[^A-Za-zА-Яа-я0-9_]+/g;

const LENGTH_EMPTY = 0;
const COUNT_ONE_STR = "1";
const RADIX_DECIMAL = 10;

const map: MapFn = (_filename, content) => {
  const out: KV[] = [];
  for (const word of content.split(WORDS)) {
    if (word.length !== LENGTH_EMPTY) {
      out.push({ key: word, value: COUNT_ONE_STR });
    }
  }
  return out;
};

const reduce: ReduceFn = (_key, values) => {
  let sum = 0;
  for (const valueText of values) {
    const parsed = Number.parseInt(valueText, RADIX_DECIMAL);
    if (Number.isFinite(parsed)) {
      sum += parsed;
    }
  }
  return String(sum);
};

export { map, reduce };
