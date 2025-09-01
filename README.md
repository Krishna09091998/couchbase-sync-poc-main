# Couchbase App Services Sync Function Deployment POC

## Overview
Automates deployment of Couchbase App Services Access Control / Validation functions (JavaScript) for multiple collections and endpoints. Deploys only updated functions based on git diff.

## Environment Configuration
- All environment-specific values (orgId, projectId, clusterId, apiKey, baseUrl) are stored in `constants.js`.
- Supports multiple environments (dev, qa, prod).
- Script dynamically picks environment using command-line argument.

## API Endpoint
- PUT https://<baseUrl>/organizations/<orgId>/projects/<projectId>/clusters/<clusterId>/endpoints/<endpoint>.<scope>.<collection>/accessControlFunction
- Uses `application/javascript` as Content-Type.
- Authorization via API key in Bearer token header.

## Prerequisites
- Node.js >= 16
- Git CLI
- Couchbase project with App Services API access

## Installation
1. Clone repository: `git clone <repo-url>`
2. Navigate: `cd <repo-root>`
3. Install dependencies: `npm install`

## Deployment
- Run script for desired environment:  
  `npm run deploy:dev`  
  `npm run deploy:qa`  
  `npm run deploy:prod`
- Script workflow:
  - Checks git diff against main branch to identify changed JS files.
  - Builds Couchbase endpoint URL dynamically.
  - Sends PUT request with JS function to App Services API.
  - Logs success/failure for each function.
- First-time run deploys all functions.

## Notes
- Only updated functions are deployed.
- Environment variables are centralized in `constants.js`.
- Future enhancement: integrate with Bitbucket/Jenkins pipeline for automatic deployment.

## References
- https://docs.couchbase.com/cloud/app-services/deployment/access-control-data-validation.html
- https://docs.couchbase.com/cloud/api.html
