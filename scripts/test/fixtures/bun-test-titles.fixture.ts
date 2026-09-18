// Registers every row of the title matrix; the tests do nothing.
import { test } from "bun:test";
import { TITLE_MATRIX } from "./bun-test-titles.matrix";

for (const [title, rows] of TITLE_MATRIX) {
  test.each(rows)(title, () => {});
}
