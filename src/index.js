const path = require("node:path");
const express = require("express");
const { BackupManager } = require("./backupManager");

async function main() {
  const app = express();
  const port = Number(process.env.PORT) || 4317;
  const publicDir = path.join(process.cwd(), "public");
  const manager = new BackupManager({
    dataDir: path.join(process.cwd(), "data"),
  });

  await manager.init();

  app.use(express.json({ limit: "1mb" }));
  app.use(express.static(publicDir));

  app.get("/api/state", (_req, res) => {
    res.json(manager.getState());
  });

  app.post("/api/config", async (req, res) => {
    try {
      const config = await manager.saveConfig(req.body || {});
      res.json({
        ok: true,
        config,
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: error.message,
      });
    }
  });

  app.post("/api/scan", async (_req, res) => {
    try {
      await manager.scanAlbums();
      res.json({
        ok: true,
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: error.message,
      });
    }
  });

  app.post("/api/oauth/start", async (req, res) => {
    try {
      if (req.body && Object.keys(req.body).length > 0) {
        await manager.saveConfig(req.body);
      }

      const callbackBaseUrl = `${req.protocol}://${req.get("host")}`;
      const result = await manager.startOAuth(callbackBaseUrl);
      res.json({
        ok: true,
        authorizeUrl: result.authorizeUrl,
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: error.message,
      });
    }
  });

  app.post("/api/backup/start", (_req, res) => {
    const state = manager.getState();
    if (!state.connection.configured) {
      res.status(400).json({
        ok: false,
        error: "Save a complete configuration before starting the backup.",
      });
      return;
    }

    if (state.backup.status === "running" || state.backup.status === "stopping") {
      res.status(409).json({
        ok: false,
        error: "A backup is already running.",
      });
      return;
    }

    manager.startBackup().catch(() => undefined);
    res.status(202).json({
      ok: true,
    });
  });

  app.post("/api/backup/stop", (_req, res) => {
    const stopping = manager.stopBackup();
    res.status(stopping ? 202 : 409).json({
      ok: stopping,
      error: stopping ? null : "There is no backup running right now.",
    });
  });

  app.get("/auth/smugmug/callback", async (req, res) => {
    try {
      const result = await manager.completeOAuth(req.query || {});
      if (!result.ok) {
        res.status(400).send(renderOAuthResultPage("Connection failed", result.message, false));
        return;
      }

      res.send(
        renderOAuthResultPage(
          "Connection complete",
          "You can now return to the app and scan your galleries.",
          true,
        ),
      );
    } catch (error) {
      res.status(500).send(renderOAuthResultPage("Connection error", error.message, false));
    }
  });

  app.get("/api/events", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();

    const send = (payload) => {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    send(manager.getState());

    let dirty = false;
    let sending = false;

    const flush = () => {
      if (!dirty || sending) {
        return;
      }

      sending = true;
      dirty = false;
      try {
        send(manager.getState());
      } finally {
        sending = false;
      }
    };

    const unsubscribe = manager.onChange(() => {
      dirty = true;
    });

    const flushTimer = setInterval(flush, 500);
    const heartbeat = setInterval(() => {
      res.write(": heartbeat\n\n");
    }, 15000);

    req.on("close", () => {
      clearInterval(flushTimer);
      clearInterval(heartbeat);
      unsubscribe();
      res.end();
    });
  });

  app.use((req, res, next) => {
    if (req.path.startsWith("/api/")) {
      next();
      return;
    }
    res.sendFile(path.join(publicDir, "index.html"));
  });

  app.listen(port, () => {
    console.log(`SmugMug Backup is available at http://localhost:${port}`);
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

function renderOAuthResultPage(title, message, success) {
  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>${escapeHtml(title)}</title>
      <style>
        body {
          margin: 0;
          font-family: "Segoe UI", sans-serif;
          background: linear-gradient(180deg, #f7f2ea 0%, #efe6d9 100%);
          color: #1f2430;
          display: grid;
          place-items: center;
          min-height: 100vh;
          padding: 24px;
        }
        .card {
          max-width: 560px;
          background: rgba(255,252,247,.95);
          border: 1px solid rgba(31,36,48,.1);
          border-radius: 24px;
          padding: 28px;
          box-shadow: 0 18px 50px rgba(54,44,30,.12);
        }
        h1 { margin-top: 0; }
        .pill {
          display: inline-block;
          padding: 8px 12px;
          border-radius: 999px;
          background: ${success ? "rgba(29,107,87,.12); color:#1d6b57;" : "rgba(169,62,53,.12); color:#a93e35;"}
        }
      </style>
    </head>
    <body>
      <div class="card">
        <div class="pill">${success ? "Success" : "Error"}</div>
        <h1>${escapeHtml(title)}</h1>
        <p>${escapeHtml(message)}</p>
        <p>This window can now be closed.</p>
      </div>
    </body>
  </html>`;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
