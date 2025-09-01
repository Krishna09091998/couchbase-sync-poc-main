const fs = require("fs");
const path = require("path");
const axios = require("axios");
const { execSync } = require("child_process");
const constants = require("../constants"); // Make sure path is correct

// Get environment from CLI argument: dev, qa, prod
const env = process.argv[2] || "dev";
const config = constants[env];

if (!config) {
  console.error(`Environment "${env}" not found in constants.js`);
  process.exit(1);
}

console.log("Using environment:", env);
console.log("Config:", config);

// Root folder containing your collection JS files
const SRC_DIR = path.join(__dirname, "../src");

// Get changed files using git diff (compared to main branch)
function getChangedFiles() {
  try {
    const output = execSync("git diff --name-only main", { encoding: "utf-8" });
    return output
      .split("\n")
      .filter(
        file =>
          file.endsWith(".js") &&
          file.startsWith("src" + path.sep) // Only src folder
      )
      .map(file => path.resolve(file));
  } catch (err) {
    console.error("Error getting git diff:", err.message);
    return [];
  }
}

// Recursively get all JS files in src folder
function getAllJsFiles(dir) {
  let files = [];
  fs.readdirSync(dir).forEach(file => {
    const fullPath = path.join(dir, file);
    if (fs.statSync(fullPath).isDirectory()) {
      files = files.concat(getAllJsFiles(fullPath));
    } else if (file.endsWith(".js")) {
      files.push(fullPath);
    }
  });
  return files;
}

// Construct Couchbase App Services endpoint URL
function buildUrl(filePath) {
  const relativePath = path.relative(SRC_DIR, filePath);
  const parts = relativePath.split(path.sep);
  const len = parts.length;
  if (len < 3) {
    console.warn("Skipping invalid file path (needs endpoint/scope/collection):", filePath);
    return null;
  }
  const endpoint = parts[0];
  const scope = parts[1];
  const collection = path.basename(parts[2], ".js");
  return `${config.baseUrl}/organizations/${config.orgId}/projects/${config.projectId}/clusters/${config.clusterId}/endpoints/${endpoint}.${scope}.${collection}/accessControlFunction`;
}

// Deploy JS function to Couchbase
async function deployFile(filePath) {
  const url = buildUrl(filePath);
  if (!url) return; // skip invalid paths

  const code = fs.readFileSync(filePath, "utf-8");

  console.log("Deploying file:", filePath);
  console.log("URL:", url);
  console.log("Code snippet (first 200 chars):", code.slice(0, 200));
  console.log("Authorization header:", `Bearer ${config.apiKey}`);

  try {
    await axios.put(url, code, {
      headers: {
        "Content-Type": "application/javascript",
        Authorization: `Bearer ${config.apiKey}`,
      },
    });
    console.log(`✅ Deployed: ${filePath}`);
  } catch (err) {
    console.error(`❌ Failed to deploy ${filePath}`);
    if (err.response) {
      console.error("Response status:", err.response.status);
      console.error("Response data:", err.response.data);
    } else {
      console.error(err.message);
    }
  }
}

(async () => {
  let filesToDeploy = getChangedFiles();
  if (filesToDeploy.length === 0) {
    console.log("No changes detected. Deploying all files in src/.");
    filesToDeploy = getAllJsFiles(SRC_DIR);
  } else {
    console.log("Changed files detected:", filesToDeploy);
  }

  for (const file of filesToDeploy) {
    await deployFile(file);
  }
})();
