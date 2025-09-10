// Deploy one sync function
const fs = require("fs");
const path = require("path");
const envConfig = require('../envConfig')

const {
    cluster,
    organizationId,
    projectId,
    clusterId,
    appServiceId,
    apiKey
} = envConfig;

async function deploySyncFunction(appType, appConfig, collection) {
    const { appEndpointName, bucket, scope } = appConfig;
    const collectionName = collection.name;

    const syncFunctionPath = path.resolve(collection.syncFunctionFile);
    if (!fs.existsSync(syncFunctionPath)) {
        console.error(`Sync function file not found: ${syncFunctionPath}`);
        return;
    }

    const syncFunctionCode = fs.readFileSync(syncFunctionPath, "utf-8");

    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}.${scope}.${collectionName}/accessControlFunction`;

    try {
        await fetch(url, {
            method: "PUT",
            headers: {
                "Authorization": `Bearer ${apiKey}`,
                "Content-Type": "application/json",
            },
            body: syncFunctionCode, // raw function, no stringify
        });
        console.log(`[${appType}] Deployed sync function for collection: ${collectionName}`);
    } catch (err) {
        console.error(
            `[${appType}] Failed to deploy ${collectionName}: ${err}`
        );
    }
}

module.exports = { deploySyncFunction }