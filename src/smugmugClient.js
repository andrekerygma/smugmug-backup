const crypto = require("node:crypto");
const fs = require("node:fs");
const fsPromises = require("node:fs/promises");
const path = require("node:path");
const { once } = require("node:events");
const { Readable } = require("node:stream");
const OAuth = require("oauth-1.0a");
const { ensureDir } = require("./utils");

class SmugMugClient {
  constructor(config) {
    this.apiBaseUrl = (config.apiBaseUrl || "https://api.smugmug.com").replace(/\/+$/, "");
    this.maxRetries = Number(config.maxRetries) > 0 ? Number(config.maxRetries) : 3;
    this.oauth = new OAuth({
      consumer: {
        key: config.apiKey,
        secret: config.apiSecret,
      },
      signature_method: "HMAC-SHA1",
      hash_function(baseString, key) {
        return crypto.createHmac("sha1", key).update(baseString).digest("base64");
      },
    });
    this.token = {
      key: config.userToken,
      secret: config.userSecret,
    };
  }

  buildAbsoluteUrl(pathOrUrl) {
    if (/^https?:\/\//i.test(pathOrUrl)) {
      return pathOrUrl;
    }

    if (pathOrUrl.startsWith("/")) {
      return `${this.apiBaseUrl}${pathOrUrl}`;
    }

    return `${this.apiBaseUrl}/${pathOrUrl}`;
  }

  signedHeaders(url, wantsJson = true) {
    const authorization = this.oauth.toHeader(
      this.oauth.authorize(
        {
          method: "GET",
          url,
        },
        this.token,
      ),
    );

    return {
      ...authorization,
      Accept: wantsJson ? "application/json" : "*/*",
    };
  }

  async fetchWithRetry(pathOrUrl, options = {}) {
    const {
      wantsJson = true,
      noAuth = false,
      label = "request",
    } = options;

    const url = this.buildAbsoluteUrl(pathOrUrl);
    let lastError = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt += 1) {
      const headers = noAuth
        ? { Accept: wantsJson ? "application/json" : "*/*" }
        : this.signedHeaders(url, wantsJson);
      const response = await fetch(url, {
        method: "GET",
        headers,
      }).catch((error) => {
        lastError = error;
        return null;
      });

      if (!response) {
        if (attempt >= this.maxRetries) {
          throw new Error(`Network failure while running ${label}: ${lastError.message}`);
        }
        await wait(1000 * attempt);
        continue;
      }

      if (response.status === 429) {
        const retryAfter = Number(response.headers.get("retry-after")) || 10;
        if (attempt >= this.maxRetries) {
          throw new Error(`SmugMug rate-limited the ${label} request (HTTP 429).`);
        }
        await wait(retryAfter * 1000);
        continue;
      }

      if (!response.ok) {
        const body = await safeReadText(response);
        const details = body ? ` ${body}` : "";
        if (attempt >= this.maxRetries || response.status < 500) {
          throw new Error(`Error ${response.status} in ${label}.${details}`);
        }
        await wait(1000 * attempt);
        continue;
      }

      return response;
    }

    throw new Error(`Unexpected failure while running ${label}.`);
  }

  async getJson(pathOrUrl, label) {
    const response = await this.fetchWithRetry(pathOrUrl, {
      wantsJson: true,
      label,
    });
    return response.json();
  }

  async getAuthenticatedUser() {
    const data = await this.getJson("/api/v2!authuser", "authuser");
    return {
      nickname: data?.Response?.User?.NickName || "",
      name: data?.Response?.User?.Name || "",
      webUri: data?.Response?.User?.WebUri || "",
    };
  }

  async getAllAlbums(nickname) {
    const user = await this.getJson(`/api/v2/user/${encodeURIComponent(nickname)}`, "user");
    const firstPageUri = user?.Response?.User?.Uris?.UserAlbums?.Uri;

    if (!firstPageUri) {
      throw new Error("The API did not return the user's albums endpoint.");
    }

    const albums = [];
    let nextPage = firstPageUri;

    while (nextPage) {
      const page = await this.getJson(nextPage, "albums");
      const pageAlbums = page?.Response?.Album || [];
      albums.push(...pageAlbums.map(mapAlbum));
      nextPage = page?.Response?.Pages?.NextPage || "";
    }

    return albums;
  }

  async getAlbumImages(albumImagesUri) {
    const images = [];
    let nextPage = albumImagesUri;

    while (nextPage) {
      const page = await this.getJson(nextPage, "album-images");
      const pageImages = page?.Response?.AlbumImage || [];
      images.push(...pageImages.map(mapImage));
      nextPage = page?.Response?.Pages?.NextPage || "";
    }

    return images;
  }

  async getLargestVideo(largestVideoUri) {
    const data = await this.getJson(largestVideoUri, "largest-video");
    return data?.Response?.LargestVideo || null;
  }

  async downloadFile(downloadUrl, destinationPath, options = {}) {
    const {
      expectedSize = 0,
      onProgress,
      shouldAbort,
    } = options;

    const url = this.buildAbsoluteUrl(downloadUrl);
    const response = await this.fetchWithRetry(url, {
      wantsJson: false,
      noAuth: true,
      label: "download",
    });

    const totalBytes = Number(response.headers.get("content-length")) || expectedSize || 0;
    const tempPath = `${destinationPath}.part`;

    await ensureDir(path.dirname(destinationPath));
    await fsPromises.rm(tempPath, { force: true }).catch(() => undefined);

    const fileStream = fs.createWriteStream(tempPath);
    let bytesWritten = 0;

    try {
      if (!response.body) {
        throw new Error("The download response did not include a body.");
      }

      const readable = Readable.fromWeb(response.body);
      for await (const chunk of readable) {
        if (shouldAbort?.()) {
          throw new Error("Backup stopped by the user.");
        }

        bytesWritten += chunk.length;
        if (!fileStream.write(chunk)) {
          await once(fileStream, "drain");
        }

        onProgress?.({
          bytesWritten,
          totalBytes,
        });
      }

      await new Promise((resolve, reject) => {
        fileStream.end((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });

      await fsPromises.rename(tempPath, destinationPath);
      return {
        bytesWritten,
        totalBytes,
      };
    } catch (error) {
      fileStream.destroy();
      await fsPromises.rm(tempPath, { force: true }).catch(() => undefined);
      throw error;
    }
  }
}

function mapAlbum(album) {
  return {
    key: album.AlbumKey || album.Uri || album.UrlPath,
    name: album.Name || album.Title || "Untitled",
    urlPath: album.UrlPath || album.Name || album.AlbumKey || "untitled",
    lastUpdated: album.LastUpdated || album.ImagesLastUpdated || album.DateModified || null,
    imageCount: album.ImageCount || album.TotalPhotos || album.TotalImages || 0,
    albumImagesUri: album?.Uris?.AlbumImages?.Uri || "",
  };
}

function mapImage(image) {
  return {
    key: image.ImageKey || image.UploadKey || image.FileName || crypto.randomUUID(),
    fileName: image.FileName || "",
    imageKey: image.ImageKey || "",
    archivedUri: image.ArchivedUri || "",
    archivedSize: image.ArchivedSize || 0,
    archivedMD5: image.ArchivedMD5 || "",
    isVideo: Boolean(image.IsVideo),
    processing: Boolean(image.Processing),
    status: image.Status || "",
    subStatus: image.SubStatus || "",
    largestVideoUri: image?.Uris?.LargestVideo?.Uri || "",
  };
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  SmugMugClient,
};
