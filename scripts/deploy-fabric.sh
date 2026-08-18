#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_NAME="${WORKSPACE_NAME:-amldemo}"
LAKEHOUSE_NAME="${LAKEHOUSE_NAME:-GOLD}"
CAPACITY_ID="${CAPACITY_ID:-}"
SNAPSHOT_ROOT="${SNAPSHOT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../data/snapshots/gold" && pwd)}"
FABRIC_RESOURCE="https://api.fabric.microsoft.com"
FABRIC_API="${FABRIC_RESOURCE}/v1"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

log_step() {
  log "STEP: $1"
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd" >&2
    exit 1
  fi
}

json_value() {
  local json="$1"
  local key="$2"
  python3 - "$json" "$key" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
key = sys.argv[2]
if isinstance(payload, dict):
    print(payload.get(key, ""))
else:
    print("")
PY
}

json_list_lookup() {
  local json="$1"
  local key="$2"
  local match="$3"
  python3 - "$json" "$key" "$match" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
key = sys.argv[2]
match_value = sys.argv[3].lower()
for item in payload.get("value", []):
    if str(item.get(key, "")).lower() == match_value:
        print(item.get("id", ""))
        break
else:
    print("")
PY
}

fabric_api() {
  local method="$1"
  local uri="$2"
  local body="${3:-}"
  local token

  token=$(az account get-access-token --resource "$FABRIC_RESOURCE" --query accessToken --output tsv 2>/dev/null || true)
  if [[ -z "$token" ]]; then
    echo "Unable to acquire a Fabric access token. Authenticate with 'az login' first." >&2
    exit 1
  fi

  if [[ -n "$body" ]]; then
    curl -sS -X "$method" \
      -H "Authorization: Bearer $token" \
      -H "Content-Type: application/json" \
      --data "$body" \
      "$uri"
  else
    curl -sS -X "$method" \
      -H "Authorization: Bearer $token" \
      -H "Content-Type: application/json" \
      "$uri"
  fi
}

wait_livy_idle() {
  local base_uri="$1"
  local session_id="$2"
  local attempt
  local state

  for ((attempt = 0; attempt < 120; attempt++)); do
    local response
    response=$(fabric_api GET "${base_uri}/sessions/${session_id}")
    state=$(python3 - "$response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("state", ""))
PY
)

    if [[ "$state" == "idle" ]]; then
      return 0
    fi

    if [[ "$state" =~ ^(dead|error|killed|shutting_down)$ ]]; then
      echo "Livy session ${session_id} entered state ${state}." >&2
      exit 1
    fi

    sleep 5
  done

  echo "Timed out waiting for Livy session ${session_id}." >&2
  exit 1
}

log_step "Validating prerequisites"
require_command az
require_command curl
require_command python3

if [[ ! -f "$SNAPSHOT_ROOT/manifest.json" ]]; then
  echo "Snapshot manifest not found at $SNAPSHOT_ROOT/manifest.json" >&2
  exit 1
fi
log "Snapshot manifest found at $SNAPSHOT_ROOT/manifest.json"

if ! command -v azcopy >/dev/null 2>&1; then
  echo "azcopy is required. Install it and try again." >&2
  exit 1
fi
log "azcopy detected: $(command -v azcopy)"

log_step "Resolving Fabric workspace"
workspace_response=$(fabric_api GET "${FABRIC_API}/workspaces")
workspace_id=$(python3 - "$workspace_response" "$WORKSPACE_NAME" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
name = sys.argv[2].lower()
for item in payload.get("value", []):
    if str(item.get("displayName", "")).lower() == name:
        print(item.get("id", ""))
        break
else:
    print("")
PY
)

if [[ -z "$workspace_id" ]]; then
  if [[ -z "$CAPACITY_ID" ]]; then
    echo "Workspace '${WORKSPACE_NAME}' does not exist. Set CAPACITY_ID to create it." >&2
    exit 1
  fi

  log "Workspace '${WORKSPACE_NAME}' not found; creating it with capacity '${CAPACITY_ID}'"
  create_body=$(python3 - "$WORKSPACE_NAME" "$CAPACITY_ID" <<'PY'
import json, sys
payload = {"displayName": sys.argv[1], "capacityId": sys.argv[2]}
print(json.dumps(payload))
PY
)
  workspace_response=$(fabric_api POST "${FABRIC_API}/workspaces" "$create_body")
  workspace_id=$(python3 - "$workspace_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("id", ""))
PY
)
else
  log "Using existing workspace '${WORKSPACE_NAME}' (${workspace_id})"
fi

workspace_details=$(fabric_api GET "${FABRIC_API}/workspaces/${workspace_id}")
workspace_capacity=$(python3 - "$workspace_details" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("capacityId", ""))
PY
)

if [[ -z "$workspace_capacity" ]]; then
  if [[ -z "$CAPACITY_ID" ]]; then
    echo "Workspace '${WORKSPACE_NAME}' has no Fabric capacity assigned and CAPACITY_ID was not provided." >&2
    exit 1
  fi

  log "Assigning capacity '${CAPACITY_ID}' to workspace '${WORKSPACE_NAME}'"
  assign_body=$(python3 - "$CAPACITY_ID" <<'PY'
import json, sys
print(json.dumps({"capacityId": sys.argv[1]}))
PY
)
  fabric_api POST "${FABRIC_API}/workspaces/${workspace_id}/assignToCapacity" "$assign_body" >/dev/null
fi

log_step "Resolving Lakehouse"
items_response=$(fabric_api GET "${FABRIC_API}/workspaces/${workspace_id}/items?type=Lakehouse")
lakehouse_id=$(python3 - "$items_response" "$LAKEHOUSE_NAME" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
name = sys.argv[2].lower()
for item in payload.get("value", []):
    if str(item.get("displayName", "")).lower() == name:
        print(item.get("id", ""))
        break
else:
    print("")
PY
)

if [[ -z "$lakehouse_id" ]]; then
  log "Lakehouse '${LAKEHOUSE_NAME}' not found; creating it"
  lakehouse_body=$(python3 - "$LAKEHOUSE_NAME" <<'PY'
import json, sys
print(json.dumps({"displayName": sys.argv[1], "type": "Lakehouse"}))
PY
)
  lakehouse_response=$(fabric_api POST "${FABRIC_API}/workspaces/${workspace_id}/items" "$lakehouse_body")
  lakehouse_id=$(python3 - "$lakehouse_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("id", ""))
PY
)
else
  log "Using existing Lakehouse '${LAKEHOUSE_NAME}' (${lakehouse_id})"
fi

log_step "Uploading snapshot to OneLake"
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
DEST="https://onelake.dfs.fabric.microsoft.com/${workspace_id}/${lakehouse_id}/Files/bootstrap"
log "Destination: $DEST"
azcopy login --login-type AZCLI
azcopy copy "${SNAPSHOT_ROOT}/" "$DEST" --recursive=true --overwrite=true --trusted-microsoft-suffixes="*.fabric.microsoft.com"

LIVY_BASE="${FABRIC_API}/workspaces/${workspace_id}/lakehouses/${lakehouse_id}/livyapi/versions/2023-12-01"
log_step "Starting Spark session"
session_body='{"name":"bash_gold_bootstrap","conf":{"spark.fabric.pool.name":"Starter Pool","spark.dynamicAllocation.enabled":"true"}}'
session_response=$(fabric_api POST "${LIVY_BASE}/sessions" "$session_body")
session_id=$(python3 - "$session_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("id", ""))
PY
)

if [[ -z "$session_id" ]]; then
  echo "Failed to create the Livy session." >&2
  exit 1
fi
log "Spark session created with id ${session_id}"

wait_livy_idle "$LIVY_BASE" "$session_id"
log "Spark session is idle and ready for execution"

bootstrap_file="$(cd "$(dirname "${BASH_SOURCE[0]}")/../src" && pwd)/bootstrap_gold.py"
code_body=$(python3 - "$bootstrap_file" "$workspace_id" "$lakehouse_id" <<'PY'
import json, sys
from pathlib import Path
script_path = Path(sys.argv[1])
workspace_id = sys.argv[2]
lakehouse_id = sys.argv[3]
source = script_path.read_text(encoding='utf-8')
source = source.replace("__WORKSPACE_ID__", workspace_id)
source = source.replace("__LAKEHOUSE_ID__", lakehouse_id)
print(json.dumps({"code": source, "kind": "pyspark"}))
PY
)

log_step "Submitting Spark bootstrap job"
statement_response=$(fabric_api POST "${LIVY_BASE}/sessions/${session_id}/statements" "$code_body")
statement_id=$(python3 - "$statement_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("id", ""))
PY
)

if [[ -z "$statement_id" ]]; then
  echo "Failed to create the Spark statement." >&2
  exit 1
fi
log "Spark statement created with id ${statement_id}; polling results..."

for ((attempt = 0; attempt < 360; attempt++)); do
  result_response=$(fabric_api GET "${LIVY_BASE}/sessions/${session_id}/statements/${statement_id}")
  state=$(python3 - "$result_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("state", ""))
PY
)

  if [[ "$state" == "available" ]]; then
    status=$(python3 - "$result_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(payload.get("output", {}).get("status", ""))
PY
)

    if [[ "$status" != "ok" ]]; then
      traceback=$(python3 - "$result_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
out = payload.get("output", {})
msg = out.get("evalue", "")
if msg:
    print(msg)
else:
    print("Spark statement failed.")
trace = out.get("traceback", [])
if trace:
    print("\n".join(trace))
PY
)
      echo "$traceback" >&2
      exit 1
    fi

    output_text=$(python3 - "$result_response" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
output = payload.get("output", {})
data = output.get("data", {})
text = data.get("text/plain", [])
if isinstance(text, list):
    print("\n".join(text))
else:
    print(str(text))
PY
)

    if [[ "$output_text" != *"DEPLOY_RESULT_JSON="* ]]; then
      echo "Deployment completed without the expected validation marker." >&2
      echo "$output_text" >&2
      exit 1
    fi

    log "Validation output received; deployment completed successfully"
    echo "$output_text"
    break
  fi

  if [[ "$state" =~ ^(error|cancelled)$ ]]; then
    echo "Livy statement entered state ${state}." >&2
    exit 1
  fi

  sleep 10
done

if [[ "$state" != "available" ]]; then
  echo "Timed out waiting for the Gold deployment statement." >&2
  exit 1
fi

log_step "Cleaning temporary Spark session"
fabric_api DELETE "${LIVY_BASE}/sessions/${session_id}" >/dev/null || true
log "GOLD deployment completed in workspace '${WORKSPACE_NAME}' (${workspace_id}) and Lakehouse '${LAKEHOUSE_NAME}'."
