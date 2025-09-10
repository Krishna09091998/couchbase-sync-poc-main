const { default: axios } = require('axios');
const envConfig = require('../envConfig')
const { resumeAppEndPoint, pauseAppEndPoint } = require("./PauseResumeEndpointService")
const {
    cluster,
    organizationId,
    projectId,
    clusterId,
    appServiceId,
    apiKey,
    master,
    grande,lite,resyncConfig
} = envConfig;

//resync all collections in an particular endpoint 
async function resyncCollections(appEndpointName) {
    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/resync`;
    try {
        const body = resyncConfig[appEndpointName]
        console.log("body",JSON.stringify(body));
       await axios(url, {
            method:"POST",
            headers: {
                "Authorization": `Bearer ${apiKey}`,
                "Content-Type": "application/json"
            },
            body: JSON.stringify(body)
        });
        console.log(`[${appEndpointName}]  resync success`);
    } catch (err) {
        console.error(
            `[${appEndpointName}] resync failed : ${err}`
        );
    }
}

async function getResyncStatus(appEndpointName) {
    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/resync`;
    try {
       let response =  await axios.get(url, {
            headers: {
                "Authorization": `Bearer ${apiKey}`,
                "Content-Type": "application/json"
            }
        });
        console.log(`[${appEndpointName}]  resync success`,response);
        return response.data;
    } catch (err) {
        console.error(
            `[${appEndpointName}] resync failed : ${err}`
        );
    }
}

async function resyncFlow(endpoint) {
    try {
        //pause appenedpoint 
        await pauseAppEndPoint(endpoint);
        //Start Resync 
        await resyncCollections(endpoint);
        //Check the resync status until completed (with retry limit)
        let state = "in-progress";
        let retries = 0; const MAXRETRIES = 12;
        while (state !== "completed" && retries < MAXRETRIES) {
            const resyncResponse = await getResyncStatus(endpoint);
            state = resyncResponse?.state || "in-progress";
            console.log(`Resync Status for ${endpoint}:${resyncResponse}`);
            if (state !== "completed") {
                await new Promise(r => setTimeout(r, 1000)); //10 seconds delay 
                retries++;
            }
        }
        if (state !== "completed") {
            throw new Error(`Resync for ${endpoint} did not complete maxretries reached out`)
        }
        //resume appendpoint
        await resumeAppEndPoint(endpoint);
        console.log(`completed the resync for ${endpoint}`)
    }
    catch (err) {
        console.log(`error in resync flow for ${endpoint}:`, err)
    }

}

module.exports = { resyncCollections, getResyncStatus, resyncFlow }