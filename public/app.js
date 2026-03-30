const dom = {
  form: document.getElementById("configForm"),
  apiBaseUrl: document.getElementById("apiBaseUrl"),
  destinationRoot: document.getElementById("destinationRoot"),
  concurrentDownloads: document.getElementById("concurrentDownloads"),
  apiKey: document.getElementById("apiKey"),
  apiSecret: document.getElementById("apiSecret"),
  connectButton: document.getElementById("connectButton"),
  scanButton: document.getElementById("scanButton"),
  startButton: document.getElementById("startButton"),
  stopButton: document.getElementById("stopButton"),
  accountName: document.getElementById("accountName"),
  connectionStatus: document.getElementById("connectionStatus"),
  backupStatus: document.getElementById("backupStatus"),
  albumsSummary: document.getElementById("albumsSummary"),
  downloadedSummary: document.getElementById("downloadedSummary"),
  downloadSpeed: document.getElementById("downloadSpeed"),
  currentAlbumEta: document.getElementById("currentAlbumEta"),
  overallEta: document.getElementById("overallEta"),
  overallProgressBar: document.getElementById("overallProgressBar"),
  overallProgressLabel: document.getElementById("overallProgressLabel"),
  fileProgressBar: document.getElementById("fileProgressBar"),
  fileProgressLabel: document.getElementById("fileProgressLabel"),
  currentAlbum: document.getElementById("currentAlbum"),
  currentFile: document.getElementById("currentFile"),
  albumsTableBody: document.getElementById("albumsTableBody"),
  logList: document.getElementById("logList"),
  configHints: document.getElementById("configHints"),
};

let hasHydratedInputs = false;

async function fetchState() {
  const response = await fetch("/api/state");
  const state = await response.json();
  render(state);
}

function connectEvents() {
  const events = new EventSource("/api/events");
  events.onmessage = (event) => {
    render(JSON.parse(event.data));
  };
}

function render(state) {
  renderConfig(state);
  renderHeader(state);
  renderProgress(state);
  renderAlbums(state.albums || []);
  renderLogs(state.logs || []);
  renderButtons(state);
}

function renderConfig(state) {
  if (!hasHydratedInputs) {
    dom.apiBaseUrl.value = state.config.apiBaseUrl || "https://api.smugmug.com";
    dom.destinationRoot.value = state.config.destinationRoot || "";
    dom.concurrentDownloads.value = state.config.concurrentDownloads || 4;
    hasHydratedInputs = true;
  }

  const flags = [
    state.config.hasApiKey ? "API key saved" : "API key missing",
    state.config.hasApiSecret ? "API secret saved" : "API secret missing",
    state.config.hasUserToken ? "Account authorized" : "Account not authorized yet",
    state.config.hasUserSecret ? "Account secret saved" : "Account secret missing",
    `${state.config.concurrentDownloads || 4} parallel downloads`,
  ];

  dom.configHints.innerHTML = flags
    .map((flag) => `<span class="hint-pill">${escapeHtml(flag)}</span>`)
    .join("");
}

function renderHeader(state) {
  const account = state.connection.account;
  dom.accountName.textContent = account?.nickname || "Not validated yet";
  dom.connectionStatus.textContent = translateConnection(state.connection.status);
  dom.connectionStatus.className = `pill ${statusTone(state.connection.status)}`;
}

function renderProgress(state) {
  const totals = state.backup.totals;
  const estimates = state.backup.estimates || {};
  const performance = state.backup.performance || {};
  const overallRatio = totals.albumsTotal ? totals.albumsProcessed / totals.albumsTotal : 0;
  const fileRatio = state.backup.currentFile?.totalBytes
    ? state.backup.currentFile.bytesWritten / state.backup.currentFile.totalBytes
    : 0;

  dom.backupStatus.textContent = translateBackup(state.backup.status);
  dom.albumsSummary.textContent = `${totals.albumsProcessed}/${totals.albumsTotal}`;
  dom.downloadedSummary.textContent = `${totals.filesDownloaded}`;
  dom.downloadSpeed.textContent = formatSpeed(
    performance.bytesPerSecond || 0,
    performance.itemsPerSecond || 0,
    state.backup.status,
  );
  dom.currentAlbumEta.textContent = formatEta(estimates.currentAlbumSeconds, state.backup.status);
  dom.overallEta.textContent = formatEta(estimates.overallSeconds, state.backup.status);

  dom.overallProgressBar.style.width = `${Math.min(overallRatio * 100, 100)}%`;
  dom.overallProgressLabel.textContent = `${Math.round(overallRatio * 100)}%`;

  dom.fileProgressBar.style.width = `${Math.min(fileRatio * 100, 100)}%`;
  dom.fileProgressLabel.textContent = `${Math.round(fileRatio * 100)}%`;

  dom.currentAlbum.textContent = state.backup.currentAlbum
    ? `${state.backup.currentAlbum.index}/${state.backup.currentAlbum.total} - ${state.backup.currentAlbum.urlPath}`
    : "None";

  dom.currentFile.textContent = state.backup.currentFile
    ? `${state.backup.currentFile.index}/${state.backup.currentFile.total} - ${state.backup.currentFile.name}${
        state.backup.currentFile.activeCount > 1 ? ` (${state.backup.currentFile.activeCount} active)` : ""
      }`
    : "None";
}

function renderAlbums(albums) {
  if (!albums.length) {
    dom.albumsTableBody.innerHTML = `
      <tr>
        <td colspan="5" class="empty-row">No galleries loaded yet.</td>
      </tr>
    `;
    return;
  }

  dom.albumsTableBody.innerHTML = albums
    .map((album) => {
      const summary = album.summary || {};
      return `
        <tr>
          <td><span class="status-chip ${album.status}">${translateAlbum(album.status)}</span></td>
          <td>
            <strong>${escapeHtml(album.urlPath)}</strong>
            <div class="muted">${escapeHtml(album.name)}</div>
          </td>
          <td class="mono">${escapeHtml(album.targetFolder || "")}</td>
          <td class="muted">
            Downloaded ${summary.downloaded || 0} / Skipped ${summary.skipped || 0} / Failed ${summary.failed || 0}
          </td>
          <td class="muted">${album.lastRunAt ? formatDate(album.lastRunAt) : "--"}</td>
        </tr>
      `;
    })
    .join("");
}

function renderLogs(logs) {
  if (!logs.length) {
    dom.logList.innerHTML = `<div class="log-entry muted">No messages yet.</div>`;
    return;
  }

  dom.logList.innerHTML = logs
    .map(
      (log) => `
        <div class="log-entry ${log.level}">
          <span class="log-time">${formatTime(log.at)}</span>
          <span>${escapeHtml(log.message)}</span>
        </div>
      `,
    )
    .join("");
}

function renderButtons(state) {
  const busy = state.backup.status === "running" || state.backup.status === "stopping";
  dom.scanButton.disabled = busy;
  dom.startButton.disabled = busy || !state.connection.configured;
  dom.stopButton.disabled = !busy;
  dom.connectButton.disabled = busy;
}

dom.form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const body = {
    apiBaseUrl: dom.apiBaseUrl.value,
    destinationRoot: dom.destinationRoot.value,
    concurrentDownloads: dom.concurrentDownloads.value,
    apiKey: dom.apiKey.value,
    apiSecret: dom.apiSecret.value,
  };

  const response = await fetch("/api/config", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const result = await response.json();
  if (!result.ok) {
    alert(result.error);
    return;
  }

  dom.apiKey.value = "";
  dom.apiSecret.value = "";
});

dom.connectButton.addEventListener("click", async () => {
  const body = {
    apiBaseUrl: dom.apiBaseUrl.value,
    destinationRoot: dom.destinationRoot.value,
    concurrentDownloads: dom.concurrentDownloads.value,
    apiKey: dom.apiKey.value,
    apiSecret: dom.apiSecret.value,
  };

  const response = await fetch("/api/oauth/start", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const result = await response.json();
  if (!result.ok) {
    alert(result.error);
    return;
  }

  window.location.href = result.authorizeUrl;
});

dom.scanButton.addEventListener("click", async () => {
  const response = await fetch("/api/scan", {
    method: "POST",
  });
  const result = await response.json();
  if (!result.ok) {
    alert(result.error);
  }
});

dom.startButton.addEventListener("click", async () => {
  const response = await fetch("/api/backup/start", {
    method: "POST",
  });
  const result = await response.json();
  if (!result.ok) {
    alert(result.error);
  }
});

dom.stopButton.addEventListener("click", async () => {
  const response = await fetch("/api/backup/stop", {
    method: "POST",
  });
  const result = await response.json();
  if (!result.ok && result.error) {
    alert(result.error);
  }
});

function translateConnection(status) {
  switch (status) {
    case "connected":
      return "Connected";
    case "validating":
      return "Validating";
    case "authorizing":
      return "Waiting for authorization";
    case "error":
      return "Error";
    default:
      return "Waiting";
  }
}

function translateBackup(status) {
  switch (status) {
    case "running":
      return "Running";
    case "stopping":
      return "Stopping";
    case "done":
      return "Done";
    case "stopped":
      return "Stopped";
    case "error":
      return "Failed";
    default:
      return "Idle";
  }
}

function translateAlbum(status) {
  switch (status) {
    case "up_to_date":
      return "Ready";
    case "running":
      return "Running";
    case "completed":
      return "Completed";
    case "partial":
      return "Partial";
    case "error":
      return "Error";
    default:
      return "Pending";
  }
}

function statusTone(status) {
  switch (status) {
    case "connected":
      return "good";
    case "error":
      return "bad";
    case "validating":
      return "warn";
    default:
      return "neutral";
  }
}

function formatDate(value) {
  return new Date(value).toLocaleString("en-US");
}

function formatTime(value) {
  return new Date(value).toLocaleTimeString("en-US");
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatSpeed(bytesPerSecond, itemsPerSecond, backupStatus) {
  if (bytesPerSecond > 0) {
    return `${formatBytes(bytesPerSecond)}/s`;
  }

  if (itemsPerSecond > 0) {
    return "No download (verifying)";
  }

  return backupStatus === "running" || backupStatus === "stopping" ? "Calculating..." : "--";
}

function formatEta(seconds, backupStatus) {
  if (Number.isFinite(seconds) && seconds > 0) {
    return formatDuration(seconds);
  }

  return backupStatus === "running" || backupStatus === "stopping" ? "Calculating..." : "--";
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  const decimals = unitIndex === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(decimals)} ${units[unitIndex]}`;
}

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Math.round(totalSeconds));
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;

  if (days > 0) {
    return `${days}d ${hours}h`;
  }

  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }

  if (minutes > 0) {
    return `${minutes}m ${secs}s`;
  }

  return `${secs}s`;
}

fetchState();
connectEvents();
