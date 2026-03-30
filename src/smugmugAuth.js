const crypto = require("node:crypto");
const OAuth = require("oauth-1.0a");

class SmugMugAuth {
  constructor(config) {
    this.apiKey = config.apiKey;
    this.apiSecret = config.apiSecret;
    this.oauth = new OAuth({
      consumer: {
        key: this.apiKey,
        secret: this.apiSecret,
      },
      signature_method: "HMAC-SHA1",
      hash_function(baseString, key) {
        return crypto.createHmac("sha1", key).update(baseString).digest("base64");
      },
    });
    this.requestTokenUrl = "https://api.smugmug.com/services/oauth/1.0a/getRequestToken";
    this.authorizeUrl = "https://api.smugmug.com/services/oauth/1.0a/authorize";
    this.accessTokenUrl = "https://api.smugmug.com/services/oauth/1.0a/getAccessToken";
  }

  async getRequestToken(callbackUrl) {
    const body = new URLSearchParams({
      oauth_callback: callbackUrl,
    });

    const response = await fetch(this.requestTokenUrl, {
      method: "POST",
      headers: this.buildHeaders(
        {
          url: this.requestTokenUrl,
          method: "POST",
          data: {
            oauth_callback: callbackUrl,
          },
        },
        null,
      ),
      body,
    });

    const raw = await response.text();
    if (!response.ok) {
      throw new Error(`Could not get the request token from SmugMug. ${raw}`);
    }

    const params = new URLSearchParams(raw);
    const requestToken = params.get("oauth_token");
    const requestTokenSecret = params.get("oauth_token_secret");
    const callbackConfirmed = params.get("oauth_callback_confirmed");

    if (!requestToken || !requestTokenSecret || callbackConfirmed !== "true") {
      throw new Error("SmugMug did not return a valid request token.");
    }

    return {
      requestToken,
      requestTokenSecret,
      callbackConfirmed,
    };
  }

  buildAuthorizeUrl(requestToken) {
    const url = new URL(this.authorizeUrl);
    url.searchParams.set("oauth_token", requestToken);
    url.searchParams.set("Access", "Full");
    url.searchParams.set("Permissions", "Read");
    return url.toString();
  }

  async getAccessToken(requestToken, requestTokenSecret, verifier) {
    const body = new URLSearchParams({
      oauth_verifier: verifier,
    });

    const response = await fetch(this.accessTokenUrl, {
      method: "POST",
      headers: this.buildHeaders(
        {
          url: this.accessTokenUrl,
          method: "POST",
          data: {
            oauth_verifier: verifier,
          },
        },
        {
          key: requestToken,
          secret: requestTokenSecret,
        },
      ),
      body,
    });

    const raw = await response.text();
    if (!response.ok) {
      throw new Error(`Could not get the access token from SmugMug. ${raw}`);
    }

    const params = new URLSearchParams(raw);
    const accessToken = params.get("oauth_token");
    const accessTokenSecret = params.get("oauth_token_secret");

    if (!accessToken || !accessTokenSecret) {
      throw new Error("SmugMug did not return the expected access token and secret.");
    }

    return {
      accessToken,
      accessTokenSecret,
    };
  }

  buildHeaders(requestData, token) {
    return {
      ...this.oauth.toHeader(this.oauth.authorize(requestData, token || undefined)),
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/x-www-form-urlencoded",
    };
  }
}

module.exports = {
  SmugMugAuth,
};
