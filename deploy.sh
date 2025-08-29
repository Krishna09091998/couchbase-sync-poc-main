#!/bin/bash

# Usage: ./deploy.sh dev|qa|prod

ENV=$1

if [[ -z "$ENV" ]]; then
  echo "Please provide environment: dev | qa | prod"
  exit 1
fi

# Set environment-specific variables
case $ENV in
  dev)
    CLUSTER_URL="https://dev-cluster.apps.cloud.couchbase.com:4985"
    ADMIN="dev_admin"
    PASS="dev_password"
    ;;
  qa)
    CLUSTER_URL="https://qa-cluster.apps.cloud.couchbase.com:4985"
    ADMIN="qa_admin"
    PASS="qa_password"
    ;;
  prod)
    CLUSTER_URL="https://prod-cluster.apps.cloud.couchbase.com:4985"
    ADMIN="prod_admin"
    PASS="prod_password"
    ;;
  *)
    echo "Invalid environment. Use dev, qa, or prod."
    exit 1
    ;;
esac

echo "Deploying sync functions to $ENV environment ..."

# Detect changed files in Git
CHANGED_FILES=$(git diff --name-only HEAD~1 HEAD buckets/*.js)

if [[ -z "$CHANGED_FILES" ]]; then
  echo "No sync function files changed. Exiting."
  exit 0
fi

# Loop through changed files only
for FILE in $CHANGED_FILES; do
  BUCKET=$(basename $FILE .js)

  # Wrap JS function as JSON
  SYNC_JSON=$(jq -n --arg fn "$(cat $FILE)" '{function: $fn}')

  echo "Updating sync function for bucket: $BUCKET"

  # Deploy to Couchbase App Services
  RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -u $ADMIN:$PASS -X PUT \
    -H "Content-Type: application/json" \
    -d "$SYNC_JSON" \
    "$CLUSTER_URL/$BUCKET/_sync")

  if [ "$RESPONSE" -eq 200 ] || [ "$RESPONSE" -eq 201 ]; then
    echo "✅ Sync function updated for $BUCKET"
  else
    echo "❌ Failed to update $BUCKET (HTTP $RESPONSE)"
  fi
done
