#!/bin/bash
set -e

# =====================================
# CONFIGURATION
# =====================================
NODE="cb-node-address"
USER="Administrator"
PASS="password"

BASE_DIR="./eventing"
CONF_DIR="$BASE_DIR/functionConfigs"
CODE_DIR="$BASE_DIR/eventingFunctions"

echo "🔍 Scanning functionConfigs folder..."

for CONFIG_FILE in "$CONF_DIR"/*.json; do
  FUNC_NAME=$(basename "$CONFIG_FILE" .json)
  CODE_FILE="$CODE_DIR/$FUNC_NAME.js"

  if [ ! -f "$CODE_FILE" ]; then
    echo "⚠️ Skipping $FUNC_NAME (missing JS file)"
    continue
  fi

  echo "⚙️ Processing function: $FUNC_NAME"

  # Merge config + JS into a valid JSON payload
  PAYLOAD=$(jq --arg code "$(cat "$CODE_FILE")" '.appcode = $code' "$CONFIG_FILE")

  # Import (create/update) function
  echo "📤 Importing $FUNC_NAME ..."
  echo "$PAYLOAD" | curl -s -u "$USER:$PASS" \
    -X POST "https://$NODE:10896/api/v1/import" \
    -H "Content-Type: application/json" \
    -d @- > /dev/null

  echo "✅ Imported $FUNC_NAME"

  # Deploy function (enable + start)
  echo "🚀 Deploying $FUNC_NAME ..."
  curl -s -u "$USER:$PASS" \
    -X POST "https://$NODE:10896/api/v1/deploy/$FUNC_NAME" \
    -H "Content-Type: application/json" \
    -d '{"deployment_status": true, "processing_status": true}' >/dev/null

  echo "🎯 $FUNC_NAME deployed successfully!"
  echo "---------------------------------------------"
done

echo "🎉 All eventing functions deployed successfully."
