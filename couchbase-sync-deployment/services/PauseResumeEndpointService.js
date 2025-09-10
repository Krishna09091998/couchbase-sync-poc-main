const envConfig = require('../envConfig')
const { default: axios } = require('axios');
const {
    cluster,
    organizationId,
    projectId,
    clusterId,
    appServiceId,
    apiKey
} = envConfig;

async function pauseAppEndPoint(appEndpointName) {
    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/activationStatus`;
    try {
        const respone = await axios.delete(url, {
            headers: {
                "Authorization": `Bearer ${apiKey}`
            }
        });
        console.log(`[${appEndpointName}]  paused ,${respone.data}`);
        return respone;
    } catch (err) {
        console.error(
            `[${appEndpointName}] pause failed : ${err}`
        );
    }
}

async function resumeAppEndPoint(appEndpointName) {
    const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/activationStatus`;
    try {
        const response = await fetch (url, {
            method:"POST",
            headers: {
                "Authorization": `Bearer ${apiKey}`
            }
        });
        console.log(`[${appEndpointName}]  resumed,${response}`);
        return response;
    } catch (err) {
        console.error(
            `[${appEndpointName}] resume failed : ${err}`
        );
    }
}

module.exports = { pauseAppEndPoint, resumeAppEndPoint }