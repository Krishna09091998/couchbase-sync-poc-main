const deployConfig = require("./deploy-config.json");

/**
 * Get environment-specific configuration.
 * 
 * Reads the environment name from `process.env.environment` (defaults to DEV3).
 * Validates that the environment exists in deploy-config.json.
 * Exports the selected environment config for use in deployment scripts.
 */
const env = process.env.environment || "DEV3"; 
const envConfig = deployConfig[env];

if (!envConfig) {
  console.error(`Environment '${env}' not found in deploy-config.json. Please check your setup.`);
  process.exit(1);
}

module.exports = envConfig;
