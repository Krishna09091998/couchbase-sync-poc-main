#!/bin/bash
set -e

# ========= CONFIGURATION =========
ENVIRONMENT=${environment:-"DEV3"}

if [ -z "$ENVIRONMENT" ]; then
  echo "❌ Please set environment variable 'environment' (e.g., DEV3)"
  exit 1
fi

DEPLOY_CONFIG="deploy-config.json"
COLLECTIONS_CONFIG="collections.json"

if [ ! -f "$DEPLOY_CONFIG" ] || [ ! -f "$COLLECTIONS_CONFIG" ]; then
  echo "❌ Missing deploy-config.json or collections.json"
  exit 1
fi

# ========= LOAD ENV CONFIG =========
CLUSTER=$(jq -r ".$ENVIRONMENT.cluster" $DEPLOY_CONFIG)
ORG_ID=$(jq -r ".$ENVIRONMENT.organizationId" $DEPLOY_CONFIG)
PROJECT_ID=$(jq -r ".$ENVIRONMENT.projectId" $DEPLOY_CONFIG)
CLUSTER_ID=$(jq -r ".$ENVIRONMENT.clusterId" $DEPLOY_CONFIG)
APP_SERVICE_ID=$(jq -r ".$ENVIRONMENT.appServiceId" $DEPLOY_CONFIG)
API_KEY=$(jq -r ".$ENVIRONMENT.apiKey" $DEPLOY_CONFIG)

MASTER_APP_ENDPOINT=$(jq -r ".$ENVIRONMENT.master.appEndpointName" $DEPLOY_CONFIG)
GRANDE_APP_ENDPOINT=$(jq -r ".$ENVIRONMENT.grande.appEndpointName" $DEPLOY_CONFIG)
LITE_APP_ENDPOINT=$(jq -r ".$ENVIRONMENT.lite.appEndpointName" $DEPLOY_CONFIG)

MASTER_SCOPE=$(jq -r ".$ENVIRONMENT.master.scope" $DEPLOY_CONFIG)
GRANDE_SCOPE=$(jq -r ".$ENVIRONMENT.grande.scope" $DEPLOY_CONFIG)
LITE_SCOPE=$(jq -r ".$ENVIRONMENT.lite.scope" $DEPLOY_CONFIG)

# ========= FUNCTIONS =========
deploy_sync_function() {
  APP_TYPE=$1
  APP_ENDPOINT=$2
  SCOPE=$3
  COLLECTION_NAME=$4
  SYNC_FILE=$(realpath "$5")


  echo "using file:$SYNC_FILE"
  
  if [ ! -f "$SYNC_FILE" ]; then
    echo "❌ Sync function file not found: $SYNC_FILE"
    return
  fi

  SYNC_FUNCTION_CODE=$(cat "$SYNC_FILE")

  echo "using code:$SYNC_FUNCTION_CODE"

  URL="${CLUSTER}/organizations/${ORG_ID}/projects/${PROJECT_ID}/clusters/${CLUSTER_ID}/appservices/${APP_SERVICE_ID}/appEndpoints/${APP_ENDPOINT}.${SCOPE}.${COLLECTION_NAME}/accessControlFunction"
  echo "URL is [$URL] "

  echo "🚀 Deploying [$APP_TYPE] collection: $COLLECTION_NAME"

  RESPONSE=$(curl -X PUT "$URL" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "$SYNC_FUNCTION_CODE"
   )

  if [ "$RESPONSE" ]; then
    echo "✅ [$APP_TYPE] Deployed sync function for collection: $COLLECTION_NAME"
  else
    echo "❌ [$APP_TYPE] Failed to deploy $COLLECTION_NAME (HTTP $RESPONSE)"
  fi
}

# ========= MAIN =========
echo "🚀 Starting deployment for environment: $ENVIRONMENT"

# Master collections
jq -c '.masterCollections[]' $COLLECTIONS_CONFIG | while read -r collection; do
  NAME=$(echo $collection | jq -r '.name')
  SYNC_FILE=$(echo $collection | jq -r '.syncFunctionFile')
  deploy_sync_function "master" "$MASTER_APP_ENDPOINT" "$MASTER_SCOPE" "$NAME" "$SYNC_FILE"
done

# Grande collections
jq -c '.grandeCollections[]' $COLLECTIONS_CONFIG | while read -r collection; do
  NAME=$(echo $collection | jq -r '.name')
  SYNC_FILE=$(echo $collection | jq -r '.syncFunctionFile')
  deploy_sync_function "grande" "$GRANDE_APP_ENDPOINT" "$GRANDE_SCOPE" "$NAME" "$SYNC_FILE"
done

# Lite collections
jq -c '.liteCollections[]' $COLLECTIONS_CONFIG | while read -r collection; do
  NAME=$(echo $collection | jq -r '.name')
  SYNC_FILE=$(echo $collection | jq -r '.syncFunctionFile')
  deploy_sync_function "lite" "$LITE_APP_ENDPOINT" "$LITE_SCOPE" "$NAME" "$SYNC_FILE"
done

echo "🎉 Deployment completed"
 