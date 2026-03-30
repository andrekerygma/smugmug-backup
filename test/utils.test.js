const test = require("node:test");
const assert = require("node:assert/strict");
const {
  normalizeUrlPath,
  safeFileName,
  sanitizePathSegment,
} = require("../src/utils");

test("sanitizePathSegment removes invalid Windows characters", () => {
  assert.equal(sanitizePathSegment("Wedding: John/Amy?"), "Wedding_ John_Amy_");
});

test("normalizeUrlPath keeps folder hierarchy while sanitizing segments", () => {
  const normalized = normalizeUrlPath("Events/Party: 2024/Album?");
  assert.match(normalized, /Events[\\/]+Party_ 2024[\\/]+Album_/);
});

test("safeFileName falls back when file name is empty", () => {
  assert.equal(safeFileName("", "photo-1"), "photo-1");
});
