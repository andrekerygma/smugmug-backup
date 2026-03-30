const fs = require("node:fs/promises");
const path = require("node:path");

function sanitizePathSegment(segment) {
  const sanitized = String(segment || "")
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, "_")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[. ]+$/g, "");

  return sanitized || "_untitled";
}

function normalizeUrlPath(urlPath) {
  return String(urlPath || "")
    .split("/")
    .filter(Boolean)
    .map(sanitizePathSegment)
    .join(path.sep);
}

function safeFileName(fileName, fallback = "file") {
  const raw = sanitizePathSegment(fileName || fallback);
  return raw.replace(/[\\/]/g, "_");
}

function guessExtensionFromUri(uri) {
  try {
    const url = new URL(uri);
    return path.extname(url.pathname);
  } catch {
    return "";
  }
}

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

async function readJson(filePath, fallbackValue) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    if (error.code === "ENOENT") {
      return fallbackValue;
    }
    throw error;
  }
}

async function writeJson(filePath, data) {
  const tempPath = `${filePath}.tmp`;
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(tempPath, JSON.stringify(data, null, 2), "utf8");
  await fs.rename(tempPath, filePath);
}

async function fileExistsWithSize(filePath, expectedSize) {
  if (!Number.isFinite(expectedSize) || expectedSize <= 0) {
    return false;
  }

  try {
    const stats = await fs.stat(filePath);
    return stats.isFile() && stats.size === expectedSize;
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function limitText(text, maxLength = 280) {
  const value = String(text || "");
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, maxLength - 1)}…`;
}

module.exports = {
  ensureDir,
  fileExistsWithSize,
  guessExtensionFromUri,
  limitText,
  normalizeUrlPath,
  nowIso,
  readJson,
  safeFileName,
  sanitizePathSegment,
  writeJson,
};
