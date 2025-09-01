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
    const files = output
      .split("\n")
      .filter(file => file.endsWith(".js"))
      .map(file => path.resolve(file));
    console.log("Changed files detected:", files);
    return files;
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

// Construct Couchbase App Services endpoint URL dynamically
function buildUrl(filePath) {
  const relativePath = path.relative(SRC_DIR, filePath);
  const parts = relativePath.split(path.sep);

  if (parts.length < 3) {
    throw new Error(`Invalid file path: ${filePath}. Must be endpoint/scope/collection.js`);
  }

  const endpoint = parts[0];
  const scope = parts[1];
  const collection = path.basename(parts[2], ".js");

  const url = `${config.baseUrl}/organizations/${config.orgId}/projects/${config.projectId}/clusters/${config.clusterId}/endpoints/${endpoint}/scopes/${scope}/collections/${collection}/accessControlFunction`;
  console.log(`URL for ${filePath}:`, url);
  return url;
}

// Deploy JS function to Couchbase
async function deployFile(filePath) {
  const url = buildUrl(filePath);
  const code = fs.readFileSync(filePath, "utf-8");

  console.log(`Deploying file: ${filePath}`);
  console.log("Code snippet (first 200 chars):", code.substring(0, 200));
  console.log("Authorization header:", `Bearer ${config.apiKey}`);

  try {
    const res = await axios.put(url, { function: code }, {
      headers: {
        "Content-Type": "application/javascript",
        Authorization: `Bearer ${config.apiKey}`,
      },
    });
    console.log(`Deployed successfully: ${filePath}`, res.status);
  } catch (err) {
    console.error(`Failed to deploy ${filePath}`);
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
    console.log("No changes detected. Deploying all files.");
    filesToDeploy = getAllJsFiles(SRC_DIR);
  }

  for (const file of filesToDeploy) {
    await deployFile(file);
  }
})();
