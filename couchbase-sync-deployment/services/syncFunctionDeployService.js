const fs = require("fs");
const path = require("path");
const envConfig = require("../envConfig");

const {
  cluster,
  organizationId,
  projectId,
  clusterId,
  appServiceId,
  apiKey,
} = envConfig;

/**
 * Deploys a sync function for a given collection to Couchbase App Services.
 * - Reads the sync function file
 * - Builds the deployment URL
 * - Sends the function code via PUT request
 * @param {string} appType - App type (e.g., "master", "grande", "lite").
 * @param {Object} appConfig - App configuration.
 * @param {Object} collection - Collection configuration.
 */
async function deploySyncFunction(appType, appConfig, collection) {
  const { appEndpointName, scope } = appConfig;
  const { name: collectionName, syncFunctionFile } = collection;

  const syncFunctionPath = path.resolve(syncFunctionFile);
  if (!fs.existsSync(syncFunctionPath)) {
    console.error(`sync function file not found: ${syncFunctionPath}`);
    return;
  }

  const syncFunctionCode = fs.readFileSync(syncFunctionPath, "utf-8");

  const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}.${scope}.${collectionName}/accessControlFunction`;

  try {
    await fetch(url, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: syncFunctionCode, // raw JS function, not stringified
    });

    console.log(`[${appType}] Deployed sync function for ${collectionName}`);
  } catch (err) {
    console.error(`[${appType}] Failed to deploy ${collectionName}:`, err.message || err);
  }
}

module.exports = { deploySyncFunction };
