// Load configuration since the static files using simple require
const { deploySyncFunction } = require("./services/syncFunctionDeployService")
const { resyncFlow } = require("./services/resyncService")
const collectionsConfig = require("./collections.json");
const envConfig = require("./envConfig")

const master = envConfig.master;
const grande = envConfig.grande;
const lite = envConfig.lite;
const resyncConfig = envConfig.resyncConfig
const endpoints = [lite.appEndpointName]
// Main deploy function
async function main() {
  console.log(`🚀 Starting deployment for environment`);

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
  for (const endpoint of endpoints) {
    await resyncFlow(endpoint)
  }

  console.log("🎉 Deployment completed");
}

main();
