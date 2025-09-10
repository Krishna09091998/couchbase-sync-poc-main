const deployConfig = require("./deploy-config.json");
const env = process.env.environment || "DEV3"; 
if (!env) {
  console.error(" Please set process.env.environment (e.g., DEV3)");
  process.exit(1);
}

// Load configuration since the static files using simple require
const envConfig = deployConfig[env];
if (!envConfig) {
  console.error(`Environment '${env}' not found in deploy-config.json`);
  process.exit(1);
}
module.exports = envConfig;