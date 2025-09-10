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
 * Pause an app endpoint by disabling activation.
 *
 * @param {string} appEndpointName - The app endpoint to pause.
 */
async function pauseAppEndPoint(appEndpointName) {
  const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/activationStatus`;

  try {
    await fetch(url, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${apiKey}` },
    });

    console.log(`[${appEndpointName}] Paused successfully`);
  } catch (err) {
    console.error(`[${appEndpointName}] Failed to pause:`, err.message || err);
  }
}

/**
 * Resume an app endpoint by re-enabling activation.
 *
 * @param {string} appEndpointName - The app endpoint to resume.
 */
async function resumeAppEndPoint(appEndpointName) {
  const url = `${cluster}/organizations/${organizationId}/projects/${projectId}/clusters/${clusterId}/appservices/${appServiceId}/appEndpoints/${appEndpointName}/activationStatus`;

  try {
    await fetch(url, {
      method: "POST",
      headers: { Authorization: `Bearer ${apiKey}` },
    });

    console.log(`[${appEndpointName}] Resumed successfully`);
  } catch (err) {
    console.error(`[${appEndpointName}] Failed to resume:`, err.message || err);
  }
}

module.exports = { pauseAppEndPoint, resumeAppEndPoint };
