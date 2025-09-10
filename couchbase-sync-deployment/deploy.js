const { deploySyncFunction } = require("./services/syncFunctionDeployService")
const { resyncFlow } = require("./services/resyncService")
const collectionsConfig = require("./collections.json");
const envConfig = require("./envConfig")

const { master, grande, lite } = envConfig;
const endpoints = [master,grande,lite]

/**
 * Main deployment flow:
 *  - Deploys sync functions based on environments 
 *  - Runs resync for the given endpoints
 */
async function main() {
  console.log(`Starting deployment`);
  const startTime = Date.now(); // start time for sync functions deploymnet
  // Deploy master collections
  for (const collection of collectionsConfig.masterCollections) {
    await deploySyncFunction("master", master, collection);
  }

  // Deploy grande collections
  for (const collection of collectionsConfig.grandeCollections) {
    await deploySyncFunction("grande", grande, collection);
  }

  // Deploy lite collections
  for (const collection of collectionsConfig.liteCollections) {
    await deploySyncFunction("lite", lite, collection);
  }
  const endTime = Date.now(); // end time for sync functions deployment
  console.log(`sync functions Deployment took ${(endTime - startTime) / 1000}s`);
  //Resync the required collections
  for (const endpoint of endpoints) {
    await resyncFlow(endpoint);
  }
  

  console.log("Deployment completed");
}

main();
