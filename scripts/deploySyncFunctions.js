const fs = require("fs");
const path = require("path");
const axios = require("axios");
const { execSync } = require("child_process");
const constants = require("./constants");

// Get environment from CLI argument: dev, qa, prod
const env = process.argv[2] || "dev";
const config = constants[env];

if (!config) {
  console.error(`Environment "${env}" not found in constants.js`);
  process.exit(1);
}

// Root folder containing your collection JS files
const SRC_DIR = path.join(__dirname, "src");

// Get changed files using git diff (compared to main branch)
function getChangedFiles() {
  try {
    // Only list JS files
    const output = execSync("git diff --name-only main", { encoding: "utf-8" });
    return output
      .split("\n")
      .filter(file => file.endsWith(".js"))
      .map(file => path.resolve(file));
  } catch (err) {
    console.error("Error getting git diff:", err.message);
    return [];
  }
}

// Recursively get all JS files if git diff is empty (fallback for first run)
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
  // src/endpoint/scope/collection.js
  const parts = filePath.split(path.sep);
  const len = parts.length;
  const endpoint = parts[len - 3];
  const scope = parts[len - 2];
  const collection = path.basename(parts[len - 1], ".js");
  return `${config.baseUrl}/organizations/${config.orgId}/projects/${config.projectId}/clusters/${config.clusterId}/endpoints/${endpoint}.${scope}.${collection}/accessControlFunction`;
}

// Deploy JS function to Couchbase
async function deployFile(filePath) {
  const url = buildUrl(filePath);
  const code = fs.readFileSync(filePath, "utf-8");

  try {
    await axios.put(url, code, {
      headers: {
        "Content-Type": "application/javascript",
        Authorization: `Bearer ${config.apiKey}`,
      },
    });
    console.log(`Deployed: ${filePath}`);
  } catch (err) {
    console.error(`Failed: ${filePath}`, err.response?.data || err.message);
  }
}

(async () => {
  let filesToDeploy = getChangedFiles();
  if (filesToDeploy.length === 0) {
    console.log("No changes detected. Deploying all files.");
    filesToDeploy = getAllJsFiles(SRC_DIR);
  }

  for (const file of filesToDeploy) {
    await deployFile(file);
  }
})();
