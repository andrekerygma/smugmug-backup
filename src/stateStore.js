const { EventEmitter } = require("node:events");
const { nowIso } = require("./utils");

class StateStore extends EventEmitter {
  constructor(initialState = {}) {
    super();
    this.version = 0;
    this.state = {
      config: {
        destinationRoot: "",
        hasApiKey: false,
        hasApiSecret: false,
        hasUserToken: false,
        hasUserSecret: false,
        apiBaseUrl: "https://api.smugmug.com",
        concurrentDownloads: 4,
      },
      connection: {
        configured: false,
        status: "idle",
        account: null,
        error: null,
        lastValidatedAt: null,
      },
      scan: {
        status: "idle",
        startedAt: null,
        finishedAt: null,
        discoveredAlbums: 0,
        error: null,
      },
      backup: {
        status: "idle",
        startedAt: null,
        finishedAt: null,
        error: null,
        currentAlbum: null,
        currentFile: null,
        performance: {
          bytesPerSecond: 0,
          itemsPerSecond: 0,
        },
        estimates: {
          currentAlbumSeconds: null,
          overallSeconds: null,
          currentAlbumRemainingBytes: 0,
          overallRemainingBytes: 0,
          currentAlbumRemainingItems: 0,
          overallRemainingItems: 0,
        },
        totals: {
          albumsTotal: 0,
          albumsProcessed: 0,
          albumsSkipped: 0,
          albumsCompleted: 0,
          albumsFailed: 0,
          filesTotal: 0,
          filesDownloaded: 0,
          filesSkipped: 0,
          filesFailed: 0,
        },
      },
      albums: [],
      logs: [],
      lastUpdatedAt: nowIso(),
      ...initialState,
    };
  }

  getState() {
    return JSON.parse(JSON.stringify(this.state));
  }

  getVersion() {
    return this.version;
  }

  update(mutator) {
    mutator(this.state);
    this.state.lastUpdatedAt = nowIso();
    this.version += 1;
    this.emit("change", {
      version: this.version,
      lastUpdatedAt: this.state.lastUpdatedAt,
    });
  }

  replaceAlbums(albums) {
    this.update((state) => {
      state.albums = albums;
    });
  }

  updateAlbum(albumKey, updater) {
    this.update((state) => {
      const index = state.albums.findIndex((album) => album.key === albumKey);
      if (index === -1) {
        return;
      }
      const updatedAlbum = { ...state.albums[index] };
      updater(updatedAlbum);
      state.albums[index] = updatedAlbum;
    });
  }

  appendLog(level, message) {
    this.update((state) => {
      state.logs.unshift({
        id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
        at: nowIso(),
        level,
        message,
      });
      state.logs = state.logs.slice(0, 300);
    });
  }
}

module.exports = {
  StateStore,
};
