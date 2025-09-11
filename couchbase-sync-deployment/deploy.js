const { deploySyncFunction } = require("./services/syncFunctionDeployService")
const { resyncFlow } = require("./services/resyncService")
const collectionsConfig = require("./collections.json");
const envConfig = require("./envConfig")

const { master, grande, lite } = envConfig;
const appTypes = { master, grande, lite };

// Get types from CLI args or default to all
const typesToProcess = process.argv.slice(2).length ? process.argv.slice(2) : ["master", "grande", "lite"];


/**
 * Main deployment flow:
 *  - Deploys sync functions based on environments 
 *  - Runs resync for the given endpoints
 */
async function main() {
  console.log(`Starting deployment`);
  // Master collections
  if (typesToProcess.includes("master")) {
  console.log("Starting deployment for master collections...");
  const startTime = Date.now();

  for (const collection of collectionsConfig.masterCollections) {
    await deploySyncFunction("master", master, collection);
  }

  const endTime = Date.now();
  console.log(`Master collections deployed in ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
}

// Grande collections
if (typesToProcess.includes("grande")) {
  console.log("Starting deployment for grande collections...");
  const startTime = Date.now();

  for (const collection of collectionsConfig.grandeCollections) {
    await deploySyncFunction("grande", grande, collection);
  }

  const endTime = Date.now();
  console.log(`Grande collections deployed in ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
}

// Lite collections
if (typesToProcess.includes("lite")) {
  console.log("Starting deployment for lite collections...");
  const startTime = Date.now();

  for (const collection of collectionsConfig.liteCollections) {
    await deploySyncFunction("lite", lite, collection);
  }

  const endTime = Date.now();
  console.log(`Lite collections deployed in ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
}
//Resync the required collections
  for (const type of typesToProcess) {
    const endpoint = appTypes[type];
    if (!endpoint) continue;
    await resyncFlow(endpoint);
  }
  console.log("Deployment completed");
}

main();
