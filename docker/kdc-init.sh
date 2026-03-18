#!/bin/sh
# kdc-init.sh — wait for KDC, create principals, export keytab
set -eu

REALM="${KRB5_REALM:-EXAMPLE.COM}"
KEYTAB_DIR="${KEYTAB_DIR:-/keytabs}"
BACKEND_PRINCIPAL="backend@${REALM}"
IMPALA_PRINCIPAL="impala/hive@${REALM}"
KEYTAB_PATH="${KEYTAB_DIR}/backend.keytab"

echo "Waiting for KDC database to be ready..."
# The KDC database file is created by the entrypoint of gcavalcante8808/krb5-server
until /usr/sbin/kadmin.local -q "listprincs" > /dev/null 2>&1; do
    echo "  KDC not ready yet, retrying in 2s..."
    sleep 2
done
echo "KDC is ready."

echo "Creating principal: ${BACKEND_PRINCIPAL}"
/usr/sbin/kadmin.local -q "addprinc -randkey ${BACKEND_PRINCIPAL}" || true

echo "Creating principal: ${IMPALA_PRINCIPAL}"
/usr/sbin/kadmin.local -q "addprinc -randkey ${IMPALA_PRINCIPAL}" || true

echo "Exporting keytab for ${BACKEND_PRINCIPAL} to ${KEYTAB_PATH}"
mkdir -p "${KEYTAB_DIR}"
/usr/sbin/kadmin.local -q "ktadd -k ${KEYTAB_PATH} ${BACKEND_PRINCIPAL}"
chmod 600 "${KEYTAB_PATH}"

echo "kdc-init complete."
