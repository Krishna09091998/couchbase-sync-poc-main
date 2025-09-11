#!/bin/bash
set -e

# ========= CONFIGURATION =========
ENVIRONMENT=${environment:-"DEV3"}

if [ -z "$ENVIRONMENT" ]; then
  echo "Please set environment variable 'environment' (e.g., DEV3)"
  exit 1
fi

DEPLOY_CONFIG="deploy-config.json"
COLLECTIONS_CONFIG="collections.json"

if [ ! -f "$DEPLOY_CONFIG" ] || [ ! -f "$COLLECTIONS_CONFIG" ]; then
  echo "Missing deploy-config.json or collections.json"
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

# Deploy one sync function
deploy_sync_function() {
  APP_TYPE=$1
  APP_ENDPOINT=$2
  SCOPE=$3
  COLLECTION_NAME=$4
  SYNC_FILE=$(realpath "$5")

  if [ ! -f "$SYNC_FILE" ]; then
    echo "Sync function file not found: $SYNC_FILE"
    return 1
  fi

  SYNC_FUNCTION_CODE=$(cat "$SYNC_FILE")
  URL="${CLUSTER}/organizations/${ORG_ID}/projects/${PROJECT_ID}/clusters/${CLUSTER_ID}/appservices/${APP_SERVICE_ID}/appEndpoints/${APP_ENDPOINT}.${SCOPE}.${COLLECTION_NAME}/accessControlFunction"

  echo "Deploying [$APP_TYPE] collection: $COLLECTION_NAME"
  RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$URL" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "$SYNC_FUNCTION_CODE")

  if [ "$RESPONSE" -eq 200 ]; then
    echo "[$APP_TYPE] Deployed sync function for collection: $COLLECTION_NAME"
  else
    echo "[$APP_TYPE] Failed to deploy $COLLECTION_NAME (HTTP $RESPONSE)"
  fi
}

# Pause endpoint
pause_endpoint() {
  APP_ENDPOINT=$1
  URL="${CLUSTER}/organizations/${ORG_ID}/projects/${PROJECT_ID}/clusters/${CLUSTER_ID}/appservices/${APP_SERVICE_ID}/appEndpoints/${APP_ENDPOINT}/activationStatus"
  curl -s -X DELETE "$URL" -H "Authorization: Bearer $API_KEY" >/dev/null
  echo "Paused $APP_ENDPOINT"
}

# Resume endpoint
resume_endpoint() {
  APP_ENDPOINT=$1
  URL="${CLUSTER}/organizations/${ORG_ID}/projects/${PROJECT_ID}/clusters/${CLUSTER_ID}/appservices/${APP_SERVICE_ID}/appEndpoints/${APP_ENDPOINT}/activationStatus"
  curl -s -X POST "$URL" -H "Authorization: Bearer $API_KEY" >/dev/null
  echo "Resumed $APP_ENDPOINT"
}

# Start resync
resync_collections() {
  APP_ENDPOINT=$1
  SCOPE=$2
  COLLECTIONS=$3
  URL="${CLUSTER}/organizations/${ORG_ID}/projects/${PROJECT_ID}/clusters/${CLUSTER_ID}/appservices/${APP_SERVICE_ID}/appEndpoints/${APP_ENDPOINT}/resync"

  BODY="{\"scopes\":{\"$SCOPE\":$COLLECTIONS}}"

  curl -s -X POST "$URL" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d "$BODY" >/dev/null

  echo "Triggered resync for $APP_ENDPOINT"
}

# Check resync status
get_resync_status() {
  APP_ENDPOINT=$1
  URL="${CLUSTER}/organizations/${ORG_ID}/projects/${PROJECT_ID}/clusters/${CLUSTER_ID}/appservices/${APP_SERVICE_ID}/appEndpoints/${APP_ENDPOINT}/resync"
  STATE=$(curl -s "$URL" -H "Authorization: Bearer $API_KEY" | jq -r '.state')
  echo "$STATE"
}

# Full resync flow
resync_flow() {
  ENDPOINT=$1
  SCOPE=$2
  COLLECTIONS=$3

  echo "Starting resync for $ENDPOINT"

  # Pause
  pause_endpoint "$ENDPOINT" || return 1

  # Handle resume if later steps fail
  {
    resync_collections "$ENDPOINT" "$SCOPE" "$COLLECTIONS" || { resume_endpoint "$ENDPOINT"; return 1; }

    local state="in-progress"
    local retries=0
    local MAXRETRIES=12

    while [[ "$state" != "completed" && $retries -lt $MAXRETRIES ]]; do
      state=$(get_resync_status "$ENDPOINT")
      echo "Resync status for $ENDPOINT: $state"
      if [[ "$state" != "completed" ]]; then
        sleep 10
        retries=$((retries + 1))
      fi
    done

    if [[ "$state" != "completed" ]]; then
      echo "Resync for $ENDPOINT did not complete (max retries reached)"
      resume_endpoint "$ENDPOINT"
      return 1
    fi

    resume_endpoint "$ENDPOINT"
    echo "Completed resync for $ENDPOINT"
  }
}

# ========= MAIN =========
echo "Starting deployment for environment: $ENVIRONMENT"

DEPLOY_START=$(date +%s)

# Deploy sync functions
jq -c '.masterCollections[]' $COLLECTIONS_CONFIG | while read -r collection; do
  NAME=$(echo $collection | jq -r '.name')
  SYNC_FILE=$(echo $collection | jq -r '.syncFunctionFile')
  deploy_sync_function "master" "$MASTER_APP_ENDPOINT" "$MASTER_SCOPE" "$NAME" "$SYNC_FILE"
done

jq -c '.grandeCollections[]' $COLLECTIONS_CONFIG | while read -r collection; do
  NAME=$(echo $collection | jq -r '.name')
  SYNC_FILE=$(echo $collection | jq -r '.syncFunctionFile')
  deploy_sync_function "grande" "$GRANDE_APP_ENDPOINT" "$GRANDE_SCOPE" "$NAME" "$SYNC_FILE"
done

jq -c '.liteCollections[]' $COLLECTIONS_CONFIG | while read -r collection; do
  NAME=$(echo $collection | jq -r '.name')
  SYNC_FILE=$(echo $collection | jq -r '.syncFunctionFile')
  deploy_sync_function "lite" "$LITE_APP_ENDPOINT" "$LITE_SCOPE" "$NAME" "$SYNC_FILE"
done

DEPLOY_END=$(date +%s)
echo "Sync functions deployment took $((DEPLOY_END - DEPLOY_START))s"

# Run resync flows
resync_flow "$MASTER_APP_ENDPOINT" "$MASTER_SCOPE" "$(jq -r ".$ENVIRONMENT.master.resyncCollections" $DEPLOY_CONFIG)"
resync_flow "$GRANDE_APP_ENDPOINT" "$GRANDE_SCOPE" "$(jq -r ".$ENVIRONMENT.grande.resyncCollections" $DEPLOY_CONFIG)"
resync_flow "$LITE_APP_ENDPOINT" "$LITE_SCOPE" "$(jq -r ".$ENVIRONMENT.lite.resyncCollections" $DEPLOY_CONFIG)"

echo "Deployment completed"
