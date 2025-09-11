# Couchbase App Services Sync Function Deployment and resyc collections

## Overview
Automates deployment of Couchbase App Services Access Control / Validation functions (JavaScript) for multiple collections and endpoints. Resync the required collections

## Workflow
- Deploys sync functions for `master`, `grande`, `lite` collections  
- Tracks time taken for deployment  
- Pauses endpoints before resync  
- Runs resync for configured collections  
- Polls resync status until completed  
- Resumes endpoints after completion  
- On failure → endpoints are resumed automatically 

## Environment Configuration
- All environment-specific values (orgId, projectId, clusterId, apiKey, baseUrl) are stored in `deployConfig.js`.
- Supports multiple environments (dev, qa, prod).
- Script dynamically picks environment from YAML.

## API Endpoint

#SYNC FUNCTIONS ENDPOINT
- https://<baseUrl>/organizations/<orgId>/projects/<projectId>/clusters/<clusterId>/endpoints/<endpoint>.<scope>.<collection>/accessControlFunction
- Uses `application/JSON` as Content-Type.
- Authorization via API key in Bearer token header.

#RESYNC FUNCTIONS ENDPOINT 
-https://<baseUrl>/organizations/<orgId>/projects/<projectId>/clusters/<clusterId>/endpoints/<endpoint>/resync
-Uses `application/JSON` as Content-Type.
- Authorization via API key in Bearer token header.

#PAUSE RESUME ENDPOINTS
-https://<baseUrl>/organizations/<orgId>/projects/<projectId>/clusters/<clusterId>/endpoints/<endpoint>/activationStatus
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

#NODEJS SCRIPT 
- Run script for desired environment:  pass the required apptypes as mentioned below
  `node deploy.js master grande lite`

#SHELL SCRIPT 
# Ensure jq and curl are installed
# Set environment
export environment=DEV3
# Run deployment
bash scripts/deploy.sh


## Notes
- Future enhancement: integrate with Bitbucket/Jenkins pipeline for automatic deployment.

## References
- https://docs.couchbase.com/cloud/app-services/deployment/access-control-data-validation.html
- https://docs.couchbase.com/cloud/management-api-reference/index.html#tag/App-Endpoints/operation/getAppEndpointResync
-https://docs.couchbase.com/cloud/management-api-reference/index.html#tag/App-Endpoints/operation/postAppEndpointActivationStatus