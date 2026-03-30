const fs = require("node:fs");
const path = require("node:path");
const { ConfigStore } = require("./configStore");
const { SmugMugAuth } = require("./smugmugAuth");
const { SmugMugClient } = require("./smugmugClient");
const { StateStore } = require("./stateStore");
const {
  ensureDir,
  fileExistsWithSize,
  guessExtensionFromUri,
  limitText,
  normalizeUrlPath,
  nowIso,
  readJson,
  safeFileName,
  writeJson,
} = require("./utils");

class BackupManager {
  constructor(options = {}) {
    const dataDir = options.dataDir || path.join(process.cwd(), "data");
    this.store = new ConfigStore(dataDir);
    this.state = new StateStore();
    this.config = null;
    this.catalog = null;
    this.manifest = null;
    this.scanPromise = null;
    this.backupPromise = null;
    this.stopRequested = false;
    this.pendingOAuth = new Map();
    this.runtimeMetrics = null;
  }

  async init() {
    await this.store.init();
    this.config = await this.store.loadConfig();
    this.catalog = await this.store.loadCatalog();
    this.manifest = await this.store.loadManifest();
    await this.hydrateState();
  }

  onChange(listener) {
    this.state.on("change", listener);
    return () => this.state.off("change", listener);
  }

  getState() {
    return this.state.getState();
  }

  async saveConfig(input) {
    const current = this.config || {
      apiBaseUrl: "https://api.smugmug.com",
      destinationRoot: "",
      apiKey: "",
      apiSecret: "",
      userToken: "",
      userSecret: "",
      concurrentDownloads: 4,
    };

    const normalized = {
      apiBaseUrl: normalizeApiBaseUrl(input.apiBaseUrl || current.apiBaseUrl),
      destinationRoot: input.destinationRoot
        ? path.resolve(String(input.destinationRoot))
        : current.destinationRoot,
      apiKey: input.apiKey ? String(input.apiKey).trim() : current.apiKey,
      apiSecret: input.apiSecret ? String(input.apiSecret).trim() : current.apiSecret,
      userToken: input.userToken ? String(input.userToken).trim() : current.userToken,
      userSecret: input.userSecret ? String(input.userSecret).trim() : current.userSecret,
      concurrentDownloads: normalizeConcurrency(input.concurrentDownloads, current.concurrentDownloads),
    };

    if (normalized.destinationRoot) {
      await ensureDir(normalized.destinationRoot);
    }

    this.config = await this.store.saveConfig(normalized);
    await this.hydrateState();
    this.state.appendLog("info", "Configuration updated.");

    return this.store.toSafeConfig(this.config);
  }

  async scanAlbums() {
    if (this.backupPromise) {
      throw new Error("Cannot scan while a backup is in progress.");
    }

    if (!this.scanPromise) {
      this.scanPromise = this.runScan().finally(() => {
        this.scanPromise = null;
      });
    }

    return this.scanPromise;
  }

  async startBackup() {
    if (this.backupPromise) {
      throw new Error("A backup is already running.");
    }

    this.backupPromise = this.runBackup().finally(() => {
      this.backupPromise = null;
    });

    return this.backupPromise;
  }

  stopBackup() {
    if (!this.backupPromise) {
      return false;
    }

    this.stopRequested = true;
    this.state.update((state) => {
      state.backup.status = "stopping";
    });
    this.state.appendLog("warn", "Stop requested. The backup will stop after the current file.");
    return true;
  }

  async hydrateState() {
    const safeConfig = this.store.toSafeConfig(this.config);
    const albums = (this.catalog?.albums || []).map((album) => ({
      ...album,
      targetFolder: album.targetFolder || path.join(safeConfig.destinationRoot || "", normalizeUrlPath(album.urlPath)),
    }));

    this.state.update((state) => {
      state.config = safeConfig;
      state.connection.configured = isConfigured(this.config);
      state.connection.account = this.catalog?.account || null;
      state.albums = albums;
    });
  }

  createClient() {
    if (!isConfigured(this.config)) {
      throw new Error("Enter the API key, API secret, user token, user secret, and destination folder before continuing.");
    }

    return new SmugMugClient(this.config);
  }

  createAuthClient() {
    if (!this.config?.apiKey || !this.config?.apiSecret) {
      throw new Error("Enter and save the API key and API secret before connecting to SmugMug.");
    }

    return new SmugMugAuth(this.config);
  }

  async startOAuth(callbackBaseUrl) {
    const auth = this.createAuthClient();
    const callbackUrl = `${callbackBaseUrl.replace(/\/+$/, "")}/auth/smugmug/callback`;

    this.state.update((state) => {
      state.connection.status = "authorizing";
      state.connection.error = null;
    });
    this.state.appendLog("info", "Starting SmugMug authorization...");

    const requestToken = await auth.getRequestToken(callbackUrl);
    this.pendingOAuth.set(requestToken.requestToken, {
      requestToken: requestToken.requestToken,
      requestTokenSecret: requestToken.requestTokenSecret,
      createdAt: Date.now(),
    });

    return {
      authorizeUrl: auth.buildAuthorizeUrl(requestToken.requestToken),
    };
  }

  async completeOAuth(query) {
    const requestToken = String(query.oauth_token || "");
    const verifier = String(query.oauth_verifier || "");
    const denied = String(query.oauth_problem || "");

    if (denied) {
      this.state.update((state) => {
        state.connection.status = "error";
        state.connection.error = denied;
      });
      this.state.appendLog("error", `Authorization was canceled or denied: ${denied}`);
      return {
        ok: false,
        message: "Authorization was canceled or denied on SmugMug.",
      };
    }

    if (!requestToken || !verifier) {
      return {
        ok: false,
        message: "SmugMug returned without oauth_token or oauth_verifier.",
      };
    }

    const pending = this.pendingOAuth.get(requestToken);
    if (!pending) {
      return {
        ok: false,
        message: "Pending authorization not found. Start again with the Connect to SmugMug button.",
      };
    }

    const auth = this.createAuthClient();
    const token = await auth.getAccessToken(pending.requestToken, pending.requestTokenSecret, verifier);
    this.pendingOAuth.delete(requestToken);

    this.config = await this.store.saveConfig({
      ...this.config,
      userToken: token.accessToken,
      userSecret: token.accessTokenSecret,
    });
    await this.hydrateState();

    try {
      const client = new SmugMugClient(this.config);
      const account = await client.getAuthenticatedUser();
      this.catalog = {
        ...(this.catalog || { albums: [] }),
        account,
        scannedAt: this.catalog?.scannedAt || null,
        albums: this.catalog?.albums || [],
      };
      await this.store.saveCatalog(this.catalog);

      this.state.update((state) => {
        state.connection.status = "connected";
        state.connection.account = account;
        state.connection.error = null;
        state.connection.lastValidatedAt = nowIso();
      });
      this.state.appendLog("info", `Account ${account.nickname} connected to SmugMug successfully.`);
    } catch (error) {
      this.state.update((state) => {
        state.connection.status = "error";
        state.connection.error = error.message;
      });
      this.state.appendLog("error", `Tokens were saved, but validation failed: ${limitText(error.message)}`);
      return {
        ok: false,
        message: `Tokens were generated, but validation failed: ${error.message}`,
      };
    }

    return {
      ok: true,
      message: "Authorization completed successfully.",
    };
  }

  async runScan() {
    const client = this.createClient();

    this.state.update((state) => {
      state.connection.status = "validating";
      state.connection.error = null;
      state.scan.status = "running";
      state.scan.startedAt = nowIso();
      state.scan.finishedAt = null;
      state.scan.error = null;
      state.scan.discoveredAlbums = 0;
    });
    this.state.appendLog("info", "Validating credentials and loading galleries from SmugMug...");

    try {
      const account = await client.getAuthenticatedUser();
      const albums = await client.getAllAlbums(account.nickname);
      const mappedAlbums = albums
        .map((album) => this.buildAlbumRecord(album))
        .sort((left, right) => left.urlPath.localeCompare(right.urlPath, "en-US"));

      this.catalog = {
        account,
        scannedAt: nowIso(),
        albums: mappedAlbums,
      };

      await this.store.saveCatalog(this.catalog);

      this.state.update((state) => {
        state.connection.status = "connected";
        state.connection.account = account;
        state.connection.error = null;
        state.connection.lastValidatedAt = nowIso();
        state.scan.status = "done";
        state.scan.finishedAt = nowIso();
        state.scan.discoveredAlbums = mappedAlbums.length;
        state.albums = mappedAlbums;
      });
      this.state.appendLog("info", `Scan completed: ${mappedAlbums.length} galleries found.`);

      return mappedAlbums;
    } catch (error) {
      this.state.update((state) => {
        state.connection.status = "error";
        state.connection.error = error.message;
        state.scan.status = "error";
        state.scan.finishedAt = nowIso();
        state.scan.error = error.message;
      });
      this.state.appendLog("error", `Failed to scan galleries: ${limitText(error.message)}`);
      throw error;
    }
  }

  buildAlbumRecord(album) {
    const fingerprint = computeAlbumFingerprint(album);
    const targetFolder = path.join(this.config.destinationRoot || "", normalizeUrlPath(album.urlPath));
    const manifestEntry = this.manifest?.albums?.[album.key];
    const folderExists = Boolean(targetFolder && fs.existsSync(targetFolder));
    const resumableStatus =
      manifestEntry?.status === "partial" || manifestEntry?.status === "running"
        ? "partial"
        : manifestEntry?.status === "error"
          ? "error"
          : "pending";
    const upToDate =
      manifestEntry &&
      manifestEntry.status === "completed" &&
      manifestEntry.fingerprint === fingerprint &&
      folderExists;

    return {
      ...album,
      fingerprint,
      targetFolder,
      status: upToDate ? "up_to_date" : resumableStatus,
      lastBackupAt: manifestEntry?.completedAt || null,
      lastRunAt: manifestEntry?.lastRunAt || null,
      lastError: manifestEntry?.lastError || null,
      summary: manifestEntry?.summary || {
        totalFiles: 0,
        downloaded: 0,
        skipped: 0,
        failed: 0,
      },
    };
  }

  buildBackupQueue(albums) {
    const skippedCompleted = [];
    const partial = [];
    const pending = [];
    const other = [];

    for (const album of albums) {
      if (this.isAlbumCompletedLocally(album)) {
        skippedCompleted.push(album);
        continue;
      }

      if (this.isAlbumBlockingPending(album)) {
        partial.push(album);
        continue;
      }

      if (album.status === "pending") {
        pending.push(album);
        continue;
      }

      other.push(album);
    }

    return {
      partial,
      pending,
      other,
      skippedCompleted: skippedCompleted.length,
    };
  }

  isAlbumCompletedLocally(album) {
    if (!album) {
      return false;
    }

    if (album.status === "up_to_date") {
      return true;
    }

    if (album.status !== "completed") {
      return false;
    }

    return Boolean(album.targetFolder && fs.existsSync(album.targetFolder));
  }

  isAlbumBlockingPending(album) {
    if (!album) {
      return false;
    }

    return album.status === "partial" || album.status === "error" || album.status === "running";
  }

  async runBackup() {
    if (!this.catalog?.albums?.length) {
      await this.scanAlbums();
    }

    const client = this.createClient();
    this.stopRequested = false;

    const albums = this.catalog.albums || [];
    const queueInfo = this.buildBackupQueue(albums);
    const resumeQueue = queueInfo.partial;
    const pendingQueue = [...queueInfo.pending, ...queueInfo.other];

    this.state.update((state) => {
      state.backup = {
        status: "running",
        startedAt: nowIso(),
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
          albumsTotal: albums.length,
          albumsProcessed: queueInfo.skippedCompleted,
          albumsSkipped: queueInfo.skippedCompleted,
          albumsCompleted: 0,
          albumsFailed: 0,
          filesTotal: 0,
          filesDownloaded: 0,
          filesSkipped: 0,
          filesFailed: 0,
        },
      };
    });
    this.resetRuntimeMetrics();
    this.state.appendLog("info", "Backup started.");
    if (queueInfo.skippedCompleted > 0) {
      this.state.appendLog("info", `Skipping ${queueInfo.skippedCompleted} galleries already completed.`);
    }
    this.state.appendLog(
      "info",
      `Queue built: ${resumeQueue.length} resume galleries and ${queueInfo.pending.length} pending galleries.`,
    );
    if (queueInfo.other.length > 0) {
      this.state.appendLog(
        "warn",
        `${queueInfo.other.length} galleries with unexpected status were queued after resume items.`,
      );
    }

    try {
      let blockedPendingPhase = false;
      const account = await client.getAuthenticatedUser();
      this.state.update((state) => {
        state.connection.status = "connected";
        state.connection.account = account;
        state.connection.lastValidatedAt = nowIso();
        state.connection.error = null;
      });

      await this.processAlbumQueue(client, resumeQueue, queueInfo.skippedCompleted, albums.length);

      const remainingResumeAlbums = (this.catalog?.albums || []).filter((album) => this.isAlbumBlockingPending(album));
      if (!this.stopRequested && remainingResumeAlbums.length > 0) {
        blockedPendingPhase = true;
        this.state.appendLog(
          "warn",
          `There are still ${remainingResumeAlbums.length} partial or failed galleries. Pending galleries will not start in this run.`,
        );
      } else if (!this.stopRequested) {
        await this.processAlbumQueue(
          client,
          pendingQueue,
          queueInfo.skippedCompleted + resumeQueue.length,
          albums.length,
        );
      }

      const finalStatus = this.stopRequested ? "stopped" : "done";
      this.state.update((state) => {
        state.backup.status = finalStatus;
        state.backup.finishedAt = nowIso();
        state.backup.currentAlbum = null;
        state.backup.currentFile = null;
        state.backup.performance.bytesPerSecond = 0;
        state.backup.performance.itemsPerSecond = 0;
        state.backup.estimates.currentAlbumSeconds = null;
        state.backup.estimates.overallSeconds = null;
        state.backup.estimates.currentAlbumRemainingBytes = 0;
        state.backup.estimates.overallRemainingBytes = 0;
        state.backup.estimates.currentAlbumRemainingItems = 0;
        state.backup.estimates.overallRemainingItems = 0;
      });
      this.runtimeMetrics = null;
      this.state.appendLog(
        this.stopRequested || blockedPendingPhase ? "warn" : "info",
        this.stopRequested
          ? "Backup stopped by the user."
          : blockedPendingPhase
            ? "Run ended after prioritizing partial galleries. Pending galleries will wait until no partial or failed galleries remain."
            : "Backup completed.",
      );
    } catch (error) {
      this.state.update((state) => {
        state.backup.status = isStopError(error) ? "stopped" : "error";
        state.backup.finishedAt = nowIso();
        state.backup.error = error.message;
        state.backup.currentAlbum = null;
        state.backup.currentFile = null;
        state.backup.performance.bytesPerSecond = 0;
        state.backup.performance.itemsPerSecond = 0;
        state.backup.estimates.currentAlbumSeconds = null;
        state.backup.estimates.overallSeconds = null;
        state.backup.estimates.currentAlbumRemainingBytes = 0;
        state.backup.estimates.overallRemainingBytes = 0;
        state.backup.estimates.currentAlbumRemainingItems = 0;
        state.backup.estimates.overallRemainingItems = 0;
      });
      this.runtimeMetrics = null;
      this.state.appendLog("error", `Backup ended with an error: ${limitText(error.message)}`);
      if (!isStopError(error)) {
        throw error;
      }
    }
  }

  async processAlbumQueue(client, queue, startIndex, totalAlbums) {
    for (let index = 0; index < queue.length; index += 1) {
      if (this.stopRequested) {
        break;
      }

      const album = queue[index];

      try {
        await this.processAlbum(client, album, startIndex + index, totalAlbums);
      } catch (error) {
        if (isStopError(error)) {
          throw error;
        }

        const failedAlbum = {
          ...album,
          status: "error",
          lastRunAt: nowIso(),
          lastError: error.message,
          summary: album.summary || {
            totalFiles: 0,
            downloaded: 0,
            skipped: 0,
            failed: 1,
          },
        };

        await this.persistAlbum(failedAlbum, this.loadResumeFiles(album));
        this.state.updateAlbum(album.key, (item) => {
          item.status = "error";
          item.lastRunAt = failedAlbum.lastRunAt;
          item.lastError = failedAlbum.lastError;
          item.summary = failedAlbum.summary;
        });
        this.bumpBackupTotals({
          albumsProcessed: 1,
          albumsFailed: 1,
        });
        this.state.appendLog("error", `Failed to process gallery ${album.urlPath}: ${limitText(error.message)}`);
      }
    }
  }

  async processAlbum(client, album, index, totalAlbums) {
    this.assertNotStopped();

    this.state.updateAlbum(album.key, (item) => {
      item.status = "running";
      item.lastError = null;
    });

    this.state.update((state) => {
      state.backup.currentAlbum = {
        key: album.key,
        name: album.name,
        urlPath: album.urlPath,
        index: index + 1,
        total: totalAlbums,
      };
      state.backup.currentFile = null;
    });
    this.state.appendLog("info", `Processing gallery ${album.urlPath}`);

    const summary = {
      totalFiles: 0,
      downloaded: 0,
      skipped: 0,
      failed: 0,
    };
    let mediaItems = [];
    const activeTransfers = new Map();
    const resumeFiles = this.loadResumeFiles(album);

    try {
      await ensureDir(album.targetFolder);
      if (album.status === "partial" && resumeFiles.size > 0) {
        this.state.appendLog(
          "info",
          `Resuming partial gallery ${album.urlPath}: ${resumeFiles.size} manifest files will be batch-validated on disk.`,
        );
      }
      const forceRefresh = album.status === "partial" || album.status === "error";
      mediaItems = await this.loadAlbumMediaItems(client, album, forceRefresh);
      const workPlan = await this.buildAlbumWorkPlan(album, mediaItems, resumeFiles);
      summary.totalFiles = mediaItems.length;
      summary.skipped = workPlan.trustedCompleted;
      this.beginAlbumMetrics(album, workPlan.workItems, index);

      this.bumpBackupTotals({
        filesTotal: workPlan.workItems.length,
      });
      if (workPlan.trustedCompleted > 0) {
        this.bumpBackupTotals({
          filesSkipped: workPlan.trustedCompleted,
        });
      }

      if (album.status === "partial") {
        this.state.appendLog(
          "info",
          `Gallery ${album.urlPath}: ${workPlan.trustedCompleted} files confirmed locally and ${workPlan.workItems.length} remaining to process.`,
        );
        if (workPlan.workItems.length > 0) {
          this.state.appendLog(
            "info",
            `Gallery ${album.urlPath}: downloading ${workPlan.workItems.length} remaining files before marking the album complete.`,
          );
        }
      }

      const concurrency = Math.min(
        normalizeConcurrency(this.config?.concurrentDownloads, 4),
        Math.max(workPlan.workItems.length, 1),
      );
      let nextIndex = 0;

      const worker = async () => {
        while (true) {
          this.assertNotStopped();

          const itemIndex = nextIndex;
          nextIndex += 1;
          if (itemIndex >= workPlan.workItems.length) {
            return;
          }

          await this.processMediaItem({
            client,
            album,
            media: workPlan.workItems[itemIndex],
            itemIndex,
            totalItems: workPlan.workItems.length,
            summary,
            activeTransfers,
            resumeFiles,
          });
        }
      };

      const workers = Array.from({ length: concurrency }, () => worker());
      const results = await Promise.allSettled(workers);
      const fatal = results.find((result) => result.status === "rejected");
      if (fatal) {
        throw fatal.reason;
      }

      const completedFiles = summary.downloaded + summary.skipped;
      const albumFullyBackedUp =
        summary.totalFiles > 0
          ? completedFiles >= summary.totalFiles && summary.failed === 0
          : Number(album.imageCount) === 0;
      const albumStatus = albumFullyBackedUp ? "completed" : "partial";
      const lastError =
        albumStatus === "completed"
          ? null
          : summary.failed > 0
            ? "Some files failed."
            : "There are still files pending download.";

      const updatedAlbum = {
        ...album,
        status: albumStatus,
        lastBackupAt: albumStatus === "completed" ? nowIso() : album.lastBackupAt,
        lastRunAt: nowIso(),
        lastError,
        summary,
      };

      await this.persistAlbum(updatedAlbum, resumeFiles);

      this.state.updateAlbum(album.key, (item) => {
        item.status = updatedAlbum.status;
        item.lastBackupAt = updatedAlbum.lastBackupAt;
        item.lastRunAt = updatedAlbum.lastRunAt;
        item.lastError = updatedAlbum.lastError;
        item.summary = summary;
      });

      this.bumpBackupTotals({
        albumsProcessed: 1,
        albumsCompleted: albumStatus === "completed" ? 1 : 0,
        albumsFailed: albumStatus === "partial" ? 1 : 0,
      });

      this.state.appendLog(
        albumStatus === "completed" ? "info" : "warn",
        `Gallery ${album.urlPath} finished. Downloaded: ${summary.downloaded}, skipped: ${summary.skipped}, failed: ${summary.failed}.`,
      );
    } catch (error) {
      if (isStopError(error)) {
        const partialAlbum = {
          ...album,
          status: summary.downloaded > 0 || summary.skipped > 0 ? "partial" : "pending",
          lastBackupAt: album.lastBackupAt,
          lastRunAt: nowIso(),
          lastError: "Stopped before completion.",
          summary,
        };

        await this.persistAlbum(partialAlbum, resumeFiles);
        this.state.updateAlbum(album.key, (item) => {
          item.status = partialAlbum.status;
          item.lastRunAt = partialAlbum.lastRunAt;
          item.lastError = partialAlbum.lastError;
          item.summary = summary;
        });
      }

      this.refreshRuntimeState(activeTransfers);
      throw error;
    }
  }

  async processMediaItem({ client, album, media, itemIndex, totalItems, summary, activeTransfers, resumeFiles }) {
    this.assertNotStopped();

    try {
      const estimatedSize = this.getEstimatedSizeForMedia(media);
      const source = await this.resolveMediaSource(client, media);
      const destinationPath = await this.resolveDestinationPath(album.targetFolder, media, source.expectedSize);
      const displayName = path.basename(destinationPath);

      if (source.expectedSize && (await fileExistsWithSize(destinationPath, source.expectedSize))) {
        this.consumeCurrentAlbumEstimatedBytes(estimatedSize);
        this.recordCompletedItem();
        this.markMediaAsResumable(media, resumeFiles, destinationPath, album.targetFolder);
        summary.skipped += 1;
        this.bumpBackupTotals({
          filesSkipped: 1,
        });
        this.refreshRuntimeState(activeTransfers);
        return;
      }

      let lastProgressAt = 0;
      activeTransfers.set(media.key, {
        albumKey: album.key,
        name: displayName,
        index: itemIndex + 1,
        total: totalItems,
        bytesWritten: 0,
        totalBytes: source.expectedSize || 0,
        activeCount: 1,
        estimatedSize,
      });
      this.refreshRuntimeState(activeTransfers);

      const result = await client.downloadFile(source.downloadUrl, destinationPath, {
        expectedSize: source.expectedSize,
        shouldAbort: () => this.stopRequested,
        onProgress: ({ bytesWritten, totalBytes }) => {
          const now = Date.now();
          if (now - lastProgressAt < 200 && bytesWritten !== totalBytes) {
            return;
          }
          lastProgressAt = now;
          const previousTransfer = activeTransfers.get(media.key);
          const previousBytes = previousTransfer?.bytesWritten || 0;
          const actualDelta = Math.max(0, bytesWritten - previousBytes);
          const estimateDelta = Math.max(
            0,
            Math.min(bytesWritten, estimatedSize) - Math.min(previousBytes, estimatedSize),
          );
          this.recordTransferredBytes(actualDelta);
          this.consumeCurrentAlbumEstimatedBytes(estimateDelta);
          activeTransfers.set(media.key, {
            albumKey: album.key,
            name: displayName,
            index: itemIndex + 1,
            total: totalItems,
            bytesWritten,
            totalBytes: totalBytes || source.expectedSize || 0,
            activeCount: activeTransfers.size,
            estimatedSize,
          });
          this.refreshRuntimeState(activeTransfers);
        },
      });

      const finalTransfer = activeTransfers.get(media.key);
      const previousBytes = finalTransfer?.bytesWritten || 0;
      const actualResidual = Math.max(0, (result?.bytesWritten || 0) - previousBytes);
      const estimateResidual = Math.max(
        0,
        Math.min(result?.bytesWritten || 0, estimatedSize) - Math.min(previousBytes, estimatedSize),
      );
      const completionResidual = Math.max(0, estimatedSize - Math.min(result?.bytesWritten || 0, estimatedSize));
      this.recordTransferredBytes(actualResidual);
      this.consumeCurrentAlbumEstimatedBytes(estimateResidual + completionResidual);

      summary.downloaded += 1;
      this.recordCompletedItem();
      this.markMediaAsResumable(media, resumeFiles, destinationPath, album.targetFolder);
      this.bumpBackupTotals({
        filesDownloaded: 1,
      });
      this.refreshRuntimeState(activeTransfers);
    } catch (error) {
      if (this.stopRequested || isStopError(error)) {
        throw stopError();
      }

      const previousTransfer = activeTransfers.get(media.key);
      const estimatedSize = previousTransfer?.estimatedSize || this.getEstimatedSizeForMedia(media);
      const alreadyAccounted = Math.min(previousTransfer?.bytesWritten || 0, estimatedSize);
      this.consumeCurrentAlbumEstimatedBytes(Math.max(0, estimatedSize - alreadyAccounted));
      this.recordCompletedItem();
      this.unmarkResumableMedia(media, resumeFiles);
      summary.failed += 1;
      this.bumpBackupTotals({
        filesFailed: 1,
      });
      this.state.appendLog(
        "error",
        `Failed to download ${album.urlPath}/${media.fileName || media.imageKey || media.key}: ${limitText(error.message)}`,
      );
    } finally {
      activeTransfers.delete(media.key);
      this.refreshRuntimeState(activeTransfers);
    }
  }

  async resolveMediaSource(client, media) {
    if (!media.isVideo) {
      if (!media.archivedUri) {
        throw new Error("The image did not include the original download link.");
      }
      return {
        downloadUrl: media.archivedUri,
        expectedSize: media.archivedSize || 0,
      };
    }

    if (!media.largestVideoUri) {
      if (media.archivedUri) {
        return {
          downloadUrl: media.archivedUri,
          expectedSize: media.archivedSize || 0,
        };
      }
      throw new Error("The video did not include a download link.");
    }

    const video = await client.getLargestVideo(media.largestVideoUri);
    if (!video?.Url) {
      throw new Error("The API did not return the video URL.");
    }

    return {
      downloadUrl: video.Url,
      expectedSize: Number(video.Size) || media.archivedSize || 0,
    };
  }

  async resolveDestinationPath(folder, media, expectedSize) {
    const fallbackName = media.imageKey || media.key || "file";
    let fileName = safeFileName(media.fileName || fallbackName, fallbackName);

    if (!path.extname(fileName) && media.archivedUri) {
      fileName += guessExtensionFromUri(media.archivedUri);
    }

    let destinationPath = path.join(folder, fileName);
    if (!fs.existsSync(destinationPath)) {
      return destinationPath;
    }

    if (expectedSize && (await fileExistsWithSize(destinationPath, expectedSize))) {
      return destinationPath;
    }

    const ext = path.extname(fileName);
    const name = path.basename(fileName, ext);
    return path.join(folder, `${name}__${safeFileName(fallbackName, "file")}${ext}`);
  }

  async loadAlbumMediaItems(client, album, forceRefresh = false) {
    const cachePath = this.getAlbumCachePath(album.key);
    if (!forceRefresh) {
      const cached = await readJson(cachePath, null);
      if (cached?.fingerprint === album.fingerprint && Array.isArray(cached.mediaItems)) {
        return cached.mediaItems;
      }
    }

    const mediaItems = await client.getAlbumImages(album.albumImagesUri);
    await writeJson(cachePath, {
      albumKey: album.key,
      fingerprint: album.fingerprint,
      savedAt: nowIso(),
      mediaItems,
    });
    return mediaItems;
  }

  getAlbumCachePath(albumKey) {
    return path.join(this.store.rootDir, "albums", `${albumKey}.json`);
  }

  async buildAlbumWorkPlan(album, mediaItems, resumeFiles) {
    if (album.status !== "partial" || resumeFiles.size === 0) {
      return {
        trustedCompleted: 0,
        workItems: mediaItems,
      };
    }

    const completionMap = new Map();
    await runWithConcurrency(mediaItems, 48, async (media) => {
      if (!resumeFiles.has(media.key)) {
        completionMap.set(media.key, false);
        return;
      }

      completionMap.set(media.key, await this.canResumeMedia(media, resumeFiles, album.targetFolder));
    });

    const workItems = [];
    let trustedCompleted = 0;

    for (const media of mediaItems) {
      if (completionMap.get(media.key)) {
        trustedCompleted += 1;
        continue;
      }

      resumeFiles.delete(media.key);
      workItems.push(media);
    }

    return {
      trustedCompleted,
      workItems,
    };
  }

  async persistAlbum(album, resumeFiles = new Map()) {
    const index = this.catalog.albums.findIndex((item) => item.key === album.key);
    if (index !== -1) {
      this.catalog.albums[index] = album;
    }

    const serializedResume = serializeResumeFiles(resumeFiles);
    this.manifest.albums[album.key] = {
      key: album.key,
      urlPath: album.urlPath,
      fingerprint: album.fingerprint,
      status: album.status,
      completedAt: album.lastBackupAt,
      lastRunAt: album.lastRunAt,
      lastError: album.lastError,
      summary: album.summary,
      ...(album.status === "completed" ? {} : { resume: { files: serializedResume } }),
    };

    await this.store.saveCatalog(this.catalog);
    await this.store.saveManifest(this.manifest);
  }

  loadResumeFiles(album) {
    const manifestEntry = this.manifest?.albums?.[album.key];
    return new Map(Object.entries(manifestEntry?.resume?.files || {}));
  }

  async canResumeMedia(media, resumeFiles, albumFolder) {
    const existing = resumeFiles.get(media.key);
    if (!existing) {
      return false;
    }

    if (existing.archivedMD5 && media.archivedMD5 && existing.archivedMD5 !== media.archivedMD5) {
      return false;
    }

    if (existing.archivedSize && media.archivedSize && Number(existing.archivedSize) !== Number(media.archivedSize)) {
      return false;
    }

    const relativePath = existing.relativePath || existing.fileName || media.fileName || "";
    if (!relativePath) {
      return false;
    }

    const candidatePath = path.join(albumFolder, relativePath);
    const expectedSize = Number(existing.archivedSize) || Number(media.archivedSize) || 0;

    if (expectedSize > 0) {
      return fileExistsWithSize(candidatePath, expectedSize);
    }

    return fs.existsSync(candidatePath);
  }

  markMediaAsResumable(media, resumeFiles, destinationPath, albumFolder) {
    resumeFiles.set(media.key, {
      fileName: media.fileName || "",
      archivedSize: Number(media.archivedSize) || 0,
      archivedMD5: media.archivedMD5 || "",
      relativePath:
        destinationPath && albumFolder
          ? path.relative(albumFolder, destinationPath)
          : media.fileName || "",
      savedAt: nowIso(),
    });
  }

  unmarkResumableMedia(media, resumeFiles) {
    resumeFiles.delete(media.key);
  }

  bumpBackupTotals(delta) {
    this.state.update((state) => {
      for (const [key, value] of Object.entries(delta)) {
        state.backup.totals[key] += value;
      }
    });
  }

  refreshRuntimeState(activeTransfers) {
    const transfers = Array.from(activeTransfers.values()).sort((left, right) => left.index - right.index);
    const leadTransfer = transfers[0] || null;
    const aggregateBytes = transfers.reduce((sum, transfer) => sum + (transfer.bytesWritten || 0), 0);
    const aggregateTotal = transfers.reduce((sum, transfer) => sum + (transfer.totalBytes || 0), 0);
    const bytesPerSecond = this.calculateSpeedBytesPerSecond();
    const itemsPerSecond = this.calculateItemsPerSecond();
    const currentAlbumRemainingBytes = Math.max(0, Math.round(this.runtimeMetrics?.currentAlbumRemainingEstimatedBytes || 0));
    const overallRemainingBytes = Math.max(
      0,
      Math.round(currentAlbumRemainingBytes + this.estimateFutureRemainingBytes()),
    );
    const currentAlbumRemainingItems = Math.max(0, Math.round(this.runtimeMetrics?.currentAlbumRemainingItems || 0));
    const overallRemainingItems = Math.max(
      0,
      Math.round(currentAlbumRemainingItems + this.estimateFutureRemainingItems()),
    );
    const currentAlbumSeconds =
      bytesPerSecond > 0 && currentAlbumRemainingBytes > 0
        ? Math.ceil(currentAlbumRemainingBytes / bytesPerSecond)
        : itemsPerSecond > 0 && currentAlbumRemainingItems > 0
          ? Math.ceil(currentAlbumRemainingItems / itemsPerSecond)
          : null;
    const overallSeconds =
      bytesPerSecond > 0 && overallRemainingBytes > 0
        ? Math.ceil(overallRemainingBytes / bytesPerSecond)
        : itemsPerSecond > 0 && overallRemainingItems > 0
          ? Math.ceil(overallRemainingItems / itemsPerSecond)
          : null;

    this.state.update((state) => {
      state.backup.currentFile = leadTransfer
        ? {
            ...leadTransfer,
            activeCount: transfers.length,
            bytesWritten: aggregateBytes,
            totalBytes: aggregateTotal,
          }
        : null;
      state.backup.performance.bytesPerSecond = bytesPerSecond;
      state.backup.performance.itemsPerSecond = itemsPerSecond;
      state.backup.estimates.currentAlbumRemainingBytes = currentAlbumRemainingBytes;
      state.backup.estimates.overallRemainingBytes = overallRemainingBytes;
      state.backup.estimates.currentAlbumRemainingItems = currentAlbumRemainingItems;
      state.backup.estimates.overallRemainingItems = overallRemainingItems;
      state.backup.estimates.currentAlbumSeconds = currentAlbumSeconds;
      state.backup.estimates.overallSeconds = overallSeconds;
    });
  }

  resetRuntimeMetrics() {
    this.runtimeMetrics = {
      startedAtMs: Date.now(),
      transferredBytes: 0,
      speedSamples: [],
      completedItems: 0,
      itemSamples: [],
      estimatedBytesSampleTotal: 0,
      estimatedItemsSampleTotal: 0,
      currentAlbumIndex: -1,
      currentAlbumItemEstimates: new Map(),
      currentAlbumRemainingEstimatedBytes: 0,
      currentAlbumEstimatedTotalBytes: 0,
      currentAlbumRemainingItems: 0,
    };
  }

  beginAlbumMetrics(album, mediaItems, index) {
    if (!this.runtimeMetrics) {
      this.resetRuntimeMetrics();
    }

    const previousAverage = this.getAverageEstimatedBytesPerItem();
    const knownSizes = mediaItems.map((media) => estimateKnownMediaSize(media)).filter((size) => size > 0);
    const knownBytes = knownSizes.reduce((sum, size) => sum + size, 0);
    const knownCount = knownSizes.length;
    const localAverage = knownCount > 0 ? knownBytes / knownCount : previousAverage;
    const fallbackAverage = localAverage || previousAverage || 0;
    const itemEstimates = new Map();
    let totalEstimatedBytes = 0;

    for (const media of mediaItems) {
      const estimatedSize = estimateKnownMediaSize(media) || fallbackAverage || 0;
      itemEstimates.set(media.key, estimatedSize);
      totalEstimatedBytes += estimatedSize;
    }

    this.runtimeMetrics.currentAlbumIndex = index;
    this.runtimeMetrics.currentAlbumItemEstimates = itemEstimates;
    this.runtimeMetrics.currentAlbumRemainingEstimatedBytes = totalEstimatedBytes;
    this.runtimeMetrics.currentAlbumEstimatedTotalBytes = totalEstimatedBytes;
    this.runtimeMetrics.currentAlbumRemainingItems = mediaItems.length;
    this.runtimeMetrics.estimatedBytesSampleTotal += knownBytes;
    this.runtimeMetrics.estimatedItemsSampleTotal += knownCount;
    this.refreshRuntimeState(new Map());
  }

  getEstimatedSizeForMedia(media) {
    return this.runtimeMetrics?.currentAlbumItemEstimates?.get(media.key) || estimateKnownMediaSize(media) || 0;
  }

  consumeCurrentAlbumEstimatedBytes(bytes) {
    if (!this.runtimeMetrics || !Number.isFinite(bytes) || bytes <= 0) {
      return;
    }

    this.runtimeMetrics.currentAlbumRemainingEstimatedBytes = Math.max(
      0,
      this.runtimeMetrics.currentAlbumRemainingEstimatedBytes - bytes,
    );
  }

  recordTransferredBytes(bytes) {
    if (!this.runtimeMetrics || !Number.isFinite(bytes) || bytes <= 0) {
      return;
    }

    this.runtimeMetrics.transferredBytes += bytes;
    const now = Date.now();
    this.runtimeMetrics.speedSamples.push({
      atMs: now,
      bytes: this.runtimeMetrics.transferredBytes,
    });
    const cutoff = now - 15000;
    this.runtimeMetrics.speedSamples = this.runtimeMetrics.speedSamples.filter((sample) => sample.atMs >= cutoff);
  }

  recordCompletedItem() {
    if (!this.runtimeMetrics) {
      return;
    }

    this.runtimeMetrics.completedItems += 1;
    this.runtimeMetrics.currentAlbumRemainingItems = Math.max(0, this.runtimeMetrics.currentAlbumRemainingItems - 1);
    const now = Date.now();
    this.runtimeMetrics.itemSamples.push({
      atMs: now,
      items: this.runtimeMetrics.completedItems,
    });
    const cutoff = now - 15000;
    this.runtimeMetrics.itemSamples = this.runtimeMetrics.itemSamples.filter((sample) => sample.atMs >= cutoff);
  }

  calculateSpeedBytesPerSecond() {
    if (!this.runtimeMetrics) {
      return 0;
    }

    const samples = this.runtimeMetrics.speedSamples;
    if (samples.length >= 2) {
      const first = samples[0];
      const last = samples[samples.length - 1];
      const elapsedMs = last.atMs - first.atMs;
      const transferred = last.bytes - first.bytes;
      if (elapsedMs > 0 && transferred > 0) {
        return Math.round((transferred * 1000) / elapsedMs);
      }
    }

    const elapsedMs = Date.now() - this.runtimeMetrics.startedAtMs;
    if (elapsedMs > 0 && this.runtimeMetrics.transferredBytes > 0) {
      return Math.round((this.runtimeMetrics.transferredBytes * 1000) / elapsedMs);
    }

    return 0;
  }

  calculateItemsPerSecond() {
    if (!this.runtimeMetrics) {
      return 0;
    }

    const samples = this.runtimeMetrics.itemSamples;
    if (samples.length >= 2) {
      const first = samples[0];
      const last = samples[samples.length - 1];
      const elapsedMs = last.atMs - first.atMs;
      const completed = last.items - first.items;
      if (elapsedMs > 0 && completed > 0) {
        return completed / (elapsedMs / 1000);
      }
    }

    const elapsedMs = Date.now() - this.runtimeMetrics.startedAtMs;
    if (elapsedMs > 0 && this.runtimeMetrics.completedItems > 0) {
      return this.runtimeMetrics.completedItems / (elapsedMs / 1000);
    }

    return 0;
  }

  getAverageEstimatedBytesPerItem() {
    if (!this.runtimeMetrics || this.runtimeMetrics.estimatedItemsSampleTotal <= 0) {
      return 0;
    }

    return this.runtimeMetrics.estimatedBytesSampleTotal / this.runtimeMetrics.estimatedItemsSampleTotal;
  }

  estimateFutureRemainingBytes() {
    if (!this.runtimeMetrics || this.runtimeMetrics.currentAlbumIndex < 0) {
      return 0;
    }

    const averageBytesPerItem = this.getAverageEstimatedBytesPerItem();
    if (averageBytesPerItem <= 0) {
      return 0;
    }

    let remainingItems = 0;
    for (let index = this.runtimeMetrics.currentAlbumIndex + 1; index < (this.catalog?.albums?.length || 0); index += 1) {
      const album = this.catalog.albums[index];
      if (album.status === "up_to_date") {
        continue;
      }
      remainingItems += Number(album.imageCount) || 0;
    }

    return remainingItems * averageBytesPerItem;
  }

  estimateFutureRemainingItems() {
    if (!this.runtimeMetrics || this.runtimeMetrics.currentAlbumIndex < 0) {
      return 0;
    }

    let remainingItems = 0;
    for (let index = this.runtimeMetrics.currentAlbumIndex + 1; index < (this.catalog?.albums?.length || 0); index += 1) {
      const album = this.catalog.albums[index];
      if (album.status === "up_to_date") {
        continue;
      }
      remainingItems += Number(album.imageCount) || 0;
    }

    return remainingItems;
  }

  assertNotStopped() {
    if (this.stopRequested) {
      throw stopError();
    }
  }
}

function isConfigured(config) {
  return Boolean(
    config &&
      config.apiKey &&
      config.apiSecret &&
      config.userToken &&
      config.userSecret &&
      config.destinationRoot,
  );
}

function computeAlbumFingerprint(album) {
  return [
    album.key,
    album.urlPath,
    album.imageCount || 0,
    album.lastUpdated || "",
  ].join("|");
}

function estimateKnownMediaSize(media) {
  const archivedSize = Number(media?.archivedSize) || 0;
  if (archivedSize > 0) {
    return archivedSize;
  }

  return 0;
}

function serializeResumeFiles(resumeFiles) {
  if (!(resumeFiles instanceof Map)) {
    return {};
  }

  return Object.fromEntries(resumeFiles.entries());
}

async function runWithConcurrency(items, concurrency, worker) {
  const maxWorkers = Math.max(1, Math.min(Number(concurrency) || 1, items.length || 1));
  let nextIndex = 0;

  const runners = Array.from({ length: maxWorkers }, async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= items.length) {
        return;
      }

      await worker(items[currentIndex], currentIndex);
    }
  });

  await Promise.all(runners);
}

function normalizeApiBaseUrl(value) {
  const normalized = String(value || "https://api.smugmug.com").trim();
  return normalized.replace(/\/+$/, "");
}

function normalizeConcurrency(value, fallback) {
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric >= 1) {
    return Math.min(Math.floor(numeric), 12);
  }

  const safeFallback = Number(fallback);
  if (Number.isFinite(safeFallback) && safeFallback >= 1) {
    return Math.min(Math.floor(safeFallback), 12);
  }

  return 4;
}

function stopError() {
  const error = new Error("Backup stopped by the user.");
  error.code = "STOP_REQUESTED";
  return error;
}

function isStopError(error) {
  return error?.code === "STOP_REQUESTED";
}

module.exports = {
  BackupManager,
};
