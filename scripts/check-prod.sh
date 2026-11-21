#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT="${1:-/tmp/workflow-prod.yaml}"
MODE="${2:---offline}"

# Check tools without changing any cluster resources.
require() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing tool: $1" >&2; exit 1; }
}
require python3
if [[ "$MODE" != "--offline" && "$MODE" != "--server" ]]; then
  echo "Usage: $0 [output.yaml] [--offline|--server]" >&2
  exit 1
fi

python3 - "$ROOT_DIR/k8s/prod/app.yaml.template" "$OUTPUT" <<'PY'
import json, os, pathlib, re, sys, urllib.parse
required = ['APP_NAMESPACE', 'APP_IMAGE', 'TEMPORAL_TARGET', 'TEMPORAL_NAMESPACE', 'AUTH_ISSUER_URI', 'AUTH_AUDIENCE']
values = {name: os.environ.get(name, '').strip() for name in required}
missing = [name for name, value in values.items() if not value]
if missing:
    raise SystemExit('Set required values: ' + ', '.join(missing))
values['INGRESS_NAMESPACE'] = os.environ.get('INGRESS_NAMESPACE', 'ingress-nginx')
values['MONITORING_NAMESPACE'] = os.environ.get('MONITORING_NAMESPACE', 'monitoring')
for name in ['APP_NAMESPACE', 'INGRESS_NAMESPACE', 'MONITORING_NAMESPACE']:
    if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', values[name]):
        raise SystemExit(name + ' must be a Kubernetes namespace name')
if not re.fullmatch(r'[^\s]+@sha256:[a-fA-F0-9]{64}', values['APP_IMAGE']):
    raise SystemExit('APP_IMAGE must be an immutable registry image@sha256:digest')
issuer = urllib.parse.urlparse(values['AUTH_ISSUER_URI'])
if issuer.scheme != 'https' or not issuer.hostname or issuer.username or issuer.password:
    raise SystemExit('AUTH_ISSUER_URI must be an HTTPS issuer without credentials')
if not re.fullmatch(r'[A-Za-z0-9.-]+:[0-9]{1,5}', values['TEMPORAL_TARGET']):
    raise SystemExit('TEMPORAL_TARGET must be a host:port address')
if not 1 <= int(values['TEMPORAL_TARGET'].rsplit(':', 1)[1]) <= 65535:
    raise SystemExit('TEMPORAL_TARGET port is invalid')
text = pathlib.Path(sys.argv[1]).read_text()
rendered = re.sub(r'\$\{([A-Z_]+)\}', lambda match: json.dumps(values[match.group(1)]), text)
path = pathlib.Path(sys.argv[2])
path.write_text(rendered)
path.chmod(0o600)
print('Rendered ' + str(path) + '; no resources changed')
PY

if [[ "$MODE" == "--server" ]]; then
  require kubectl
  # Inspect key names and lengths privately; never print credential values.
  SERVER_DATA_DIR="$(mktemp -d)"
  trap 'rm -rf "$SERVER_DATA_DIR"' EXIT
  umask 077
  kubectl -n "$APP_NAMESPACE" get secret temporal-client workflow-adapter-auth workflow-api -o json >"$SERVER_DATA_DIR/secrets.json"
  kubectl -n "$APP_NAMESPACE" get configmap workflow-registry -o json >"$SERVER_DATA_DIR/registry.json"
  python3 - "$SERVER_DATA_DIR/secrets.json" "$SERVER_DATA_DIR/registry.json" <<'PY_CHECK'
import base64, binascii, json, pathlib, sys
secrets = json.loads(pathlib.Path(sys.argv[1]).read_text())
by_name = {item['metadata']['name']: item.get('data', {}) for item in secrets.get('items', [])}
required = {
    'temporal-client': ['tls.crt', 'tls.key'],
    'workflow-api': ['cursor-signing-key'],
    'workflow-adapter-auth': ['tokens.properties'],
}
for name, keys in required.items():
    fields = by_name.get(name, {})
    for key in keys:
        if key not in fields:
            raise SystemExit('Required Secret key is missing: ' + name + '/' + key)
        try:
            value = base64.b64decode(fields[key], validate=True)
        except (ValueError, binascii.Error):
            raise SystemExit('Required Secret key has invalid encoding: ' + name + '/' + key)
        if name == 'workflow-api':
            try:
                text = value.decode('utf-8')
            except UnicodeDecodeError:
                raise SystemExit('workflow-api/cursor-signing-key must be UTF-8 text')
            if len(value) < 32 or '\x00' in text:
                raise SystemExit('workflow-api/cursor-signing-key needs at least 32 UTF-8 bytes and no null characters')
        elif key != 'tokens.properties' and not value.strip():
            raise SystemExit('Required Secret key is empty: ' + name + '/' + key)
        elif key == 'tokens.properties':
            try:
                value.decode('utf-8')
            except UnicodeDecodeError:
                raise SystemExit('workflow-adapter-auth/tokens.properties must be UTF-8 text')
registry = json.loads(pathlib.Path(sys.argv[2]).read_text())
if not registry.get('data', {}).get('integration.properties', '').strip():
    raise SystemExit('workflow-registry/integration.properties is missing or empty')
print('Configuration keys and minimum lengths passed; credential contents were not printed')
PY_CHECK
  kubectl apply --dry-run=server -f "$OUTPUT" >/dev/null
  echo "Server validation passed. No resources were applied."
fi

echo "Before rollout: verify external Temporal, TLS ingress, JWT scopes, adapter idempotency, backups, alerts, and upgrade tests."
