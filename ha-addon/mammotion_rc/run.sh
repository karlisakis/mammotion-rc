#!/usr/bin/env bash
# Entry point for the Mammotion RC add-on.
#
# Layout: the image carries the app at /opt/mammotion-rc/web-server; on every
# start we sync it into /data/app (the add-on's persistent volume) and run it
# from there. persist.py writes its state (mowers.toml, secrets.toml) beside
# app.py — those files are never in the image, so the copy refreshes code while
# leaving state untouched, and everything survives restarts and updates.
set -euo pipefail

OPTS=/data/options.json
APP=/data/app

mkdir -p "$APP"
cp -a /opt/mammotion-rc/web-server/. "$APP/"

opt() { python3 -c "import json;v=json.load(open('$OPTS')).get('$1');print('' if v is None else v)"; }

port="$(opt port)"; port="${port:-8443}"
web_username="$(opt web_username)"
web_password="$(opt web_password)"
regen="$(opt regenerate_cert)"

if [ -n "$web_username" ]; then export LUBA_WEB_USERNAME="$web_username"; fi
if [ -n "$web_password" ]; then export LUBA_WEB_PASSWORD="$web_password"; fi
if [ -z "$web_password" ]; then
  echo "[run] WARNING: web_password option is empty — the UI is UNPROTECTED"
fi

# Self-signed TLS cert in /data (persistent). HTTPS is mandatory: the camera
# is WebRTC, which browsers only allow in a secure context. The SANs bake in
# this host's name and LAN IP, so if the HA box's IP changes, flip the
# regenerate_cert option on and restart.
if [ "$regen" = "True" ] || [ ! -f /data/cert.pem ] || [ ! -f /data/key.pem ]; then
  echo "[run] generating self-signed certificate"
  (cd "$APP" && python3 gen_cert.py --cert /data/cert.pem --key /data/key.pem)
fi

echo "[run] starting Mammotion RC on 0.0.0.0:${port} (https)"
cd "$APP"
exec python3 -m uvicorn app:app --host 0.0.0.0 --port "$port" \
  --ssl-keyfile /data/key.pem --ssl-certfile /data/cert.pem \
  --timeout-graceful-shutdown 2
