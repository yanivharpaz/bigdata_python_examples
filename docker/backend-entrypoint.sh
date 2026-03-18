#!/bin/sh
# Substitute KRB5_REALM and KRB5_KDC placeholders in krb5.conf, then start the app.
set -eu

KRB5_CONF_SRC="/etc/krb5/krb5.conf"
KRB5_CONF_DST="/etc/krb5.conf"

# Replace literal placeholder strings with actual env var values
sed \
  -e "s/KRB5_REALM/${KRB5_REALM}/g" \
  -e "s/KRB5_KDC/${KRB5_KDC}/g" \
  "${KRB5_CONF_SRC}" > "${KRB5_CONF_DST}"

export KRB5_CONFIG="${KRB5_CONF_DST}"

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
