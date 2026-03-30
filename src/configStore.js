const path = require("node:path");
const { ensureDir, readJson, writeJson } = require("./utils");

class ConfigStore {
  constructor(rootDir) {
    this.rootDir = rootDir;
    this.configPath = path.join(rootDir, "config.json");
    this.catalogPath = path.join(rootDir, "catalog.json");
    this.manifestPath = path.join(rootDir, "manifest.json");
  }

  async init() {
    await ensureDir(this.rootDir);
  }

  async loadConfig() {
    return readJson(this.configPath, {
      apiBaseUrl: "https://api.smugmug.com",
      destinationRoot: "",
      apiKey: "",
      apiSecret: "",
      userToken: "",
      userSecret: "",
      concurrentDownloads: 4,
    });
  }

  async saveConfig(config) {
    await writeJson(this.configPath, config);
    return config;
  }

  async loadCatalog() {
    return readJson(this.catalogPath, {
      account: null,
      scannedAt: null,
      albums: [],
    });
  }

  async saveCatalog(catalog) {
    await writeJson(this.catalogPath, catalog);
    return catalog;
  }

  async loadManifest() {
    return readJson(this.manifestPath, {
      albums: {},
    });
  }

  async saveManifest(manifest) {
    await writeJson(this.manifestPath, manifest);
    return manifest;
  }

  toSafeConfig(config) {
    return {
      apiBaseUrl: config.apiBaseUrl || "https://api.smugmug.com",
      destinationRoot: config.destinationRoot || "",
      hasApiKey: Boolean(config.apiKey),
      hasApiSecret: Boolean(config.apiSecret),
      hasUserToken: Boolean(config.userToken),
      hasUserSecret: Boolean(config.userSecret),
      concurrentDownloads: Number(config.concurrentDownloads) > 0 ? Number(config.concurrentDownloads) : 4,
    };
  }
}

module.exports = {
  ConfigStore,
};
