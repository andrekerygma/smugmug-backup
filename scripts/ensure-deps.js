const path = require("node:path");

const requiredModules = ["express", "oauth-1.0a"];
const missing = [];

for (const moduleName of requiredModules) {
  try {
    require.resolve(moduleName, {
      paths: [process.cwd(), path.join(process.cwd(), "node_modules")],
    });
  } catch {
    missing.push(moduleName);
  }
}

if (missing.length > 0) {
  console.error("");
  console.error("Missing dependencies:", missing.join(", "));
  console.error("Run `npm install` in this folder before starting the app.");
  console.error("");
  process.exit(1);
}
