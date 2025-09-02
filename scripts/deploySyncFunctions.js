const fs = require("fs");
const path = require("path");
const axios = require("axios");
const { execSync } = require("child_process");
const constants = require("../constants");

// Get environment from CLI argument: dev, qa, prod
const env = process.argv[2] || "dev";
const config = constants[env];

if (!config) {
  console.error(`Environment "${env}" not found in constants.js`);
  process.exit(1);
}

console.log("Using environment:", env);
console.log("Config:", config);

// Get the Root folder path 
const SRC_DIR = path.join(__dirname, "../src");

// Get changed files using git diff compared to main branch varies depedning on envoirment in real time
function getChangedFiles() {
  try {
    const output = execSync("git diff --name-only main", { encoding: "utf-8" });
    return output
      .split("\n")
      .filter(
        file =>
          file.endsWith(".js") &&
          file.startsWith("src" + path.sep)
      )
      .map(file => path.resolve(file));
  } catch (err) {
    console.error("Error getting git diff:", err.message);
    return [];
  }
}

/** 
 * Recursively get all JavaScript files from a directory and its subdirectories.
 * @param {string} dir - The directory path to scan.
 * @returns {string[]} - Array of full paths to all .js files found.
 */
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

/**
 * Construct the Couchbase App Services endpoint URL for a JS file.
 * @param {string} filePath - Full path to the JS file.
 * @returns {string|null} - Full URL to the accessControlFunction, or null if invalid path.
 */
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
  return `${config.baseUrl}/organizations/${config.orgId}/projects/${config.projectId}/clusters/${config.clusterId}/appservices/${config.appId}/appEndpoints/${endpoint}.${scope}.${collection}/accessControlFunction`;
}

/**
 * Deploy a JS file to Couchbase App Services.
 * @param {string} filePath - Full path to the JS file.
 */
async function deployFile(filePath) {
  const url = buildUrl(filePath);
  if (!url) return;

  // Read the JS code as string
  let code = fs.readFileSync(filePath, "utf-8").trim();

// If the file exports a string, extract it
if (code.startsWith("module.exports")) {
  const match = code.match(/`([\s\S]*)`/);
  if (match) {
    code = match[1].trim();
  }
}


  // Wrap in JSON string (escaped)
  const payload = code;

  console.log("Deploying file:", filePath);
  console.log("URL:", url);
  console.log("Payload preview (first 200 chars):", payload.slice(0, 200));
  console.log("Authorization header:", `Bearer ${config.apiKey}`);

  try {
    await fetch(url, {
  method: "PUT",
  headers: {
    "Authorization": `Bearer ${config.apiKey}`,
    "Content-Type": "application/json",
  },
  body: payload, // raw function, no stringify
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
