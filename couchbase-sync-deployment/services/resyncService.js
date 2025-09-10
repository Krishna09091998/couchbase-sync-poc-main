const { default: axios } = require("axios");
const envConfig = require("../envConfig");
const { resumeAppEndPoint, pauseAppEndPoint } = require("./PauseResumeEndpointService");

const {
    cluster,
    organizationId,
    projectId,
    clusterId,
    appServiceId,
    apiKey
} = envConfig;

/**
 * Triggers resync for all/reuired collections in a given endpoint.
 * Resync collections for a single endpoint.
 * @param {string} appEndpointName - Endpoint name
 * @param {string} scope - Scope name
 * @param {Array<string>} collections - List of collections to resync
 */

async function resyncCollections(appEndpointName, scope, collections) {
    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/resync`;
    try {
        await fetch(url, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${apiKey}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ scopes: { [scope]: collections } }),
        });

        console.log(`[${appEndpointName}] Resync triggered successfully`);
    } catch (err) {
        console.error(`[${appEndpointName}] Resync failed:`, err.message || err);
    }
}

/**
 * Fetches resync status for a given endpoint.
 *
 * @param {string} appEndpointName - The app endpoint to check status for.
 * @returns {Promise<Object|undefined>} The resync status response.
 */
async function getResyncStatus(appEndpointName) {
    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/resync`;
    try {
        const response = await axios.get(url, {
            headers: {
                Authorization: `Bearer ${apiKey}`,
                "Content-Type": "application/json",
            },
        });

        console.log(`[${appEndpointName}] Resync status:`, response.data);
        return response.data;
    } catch (err) {
        console.error(`[${appEndpointName}] Failed to fetch resync status:`, err.message || err);
    }
}

/**
 * Full resync flow:
 *  - Pauses the endpoint
 *  - Triggers resync
 *  - Polls status until completion
 *  - Resumes the endpoint
 *
 * @param {object} endpoint - The app endpoint data to resync.
 */
async function resyncFlow(endpoint) {
    let endpointPaused = false;
    const { appEndpointName, scope, resyncCollections: collections } = endpoint;
    const flowStartTime = Date.now();
    try {
        if (!collections || collections.length === 0) {
            console.log(`${appEndpointName} has no collections to resync. Skipping.`);
            return;
        }
        await pauseAppEndPoint(appEndpointName);
        endpointPaused = true;
        await resyncCollections(appEndpointName, scope, collections);

        let state = "in-progress";
        let retries = 0;
        const MAX_RETRIES = 12;

        while (state !== "completed" && retries < MAX_RETRIES) {
            const resyncResponse = await getResyncStatus(appEndpointName);
            state = resyncResponse?.state || "in-progress";
            console.log(`Resync Status for ${appEndpointName}: ${state}`);
            if (state !== "completed") {
                await new Promise((r) => setTimeout(r, 10_000)); // 10s delay
                retries++;
            }
        }
        if (state !== "completed") {
            throw new Error(`Resync for ${appEndpointName} did not complete (max retries reached)`);
        }
        const flowEndTime = Date.now();
        console.log(`Completed resync for ${appEndpointName}in ${(flowEndTime - flowStartTime) / 1000}s`);
    } catch (err) {
        console.error(`Error in resync flow for ${appEndpointName}:`, err.message || err);
    }
    finally {
        if (endpointPaused) {
            await resumeAppEndPoint(appEndpointName);
            console.log("Endpoint resumed");
        }
    }
}

module.exports = { resyncCollections, getResyncStatus, resyncFlow };
