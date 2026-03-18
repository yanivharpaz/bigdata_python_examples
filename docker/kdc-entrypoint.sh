#!/bin/sh
# kdc-entrypoint.sh
# Wraps the standard KDC entrypoint, then creates principals and exports keytab.
set -eu

REALM="${KRB5_REALM:-EXAMPLE.COM}"
KEYTAB_DIR="${KEYTAB_DIR:-/keytabs}"
BACKEND_PRINCIPAL="backend@${REALM}"
IMPALA_PRINCIPAL="impala/hive@${REALM}"
KEYTAB_PATH="${KEYTAB_DIR}/backend.keytab"

# Run the original entrypoint in the background to initialise the KDC database
/docker-entrypoint.sh &

echo "Waiting for KDC database to be ready..."
until /usr/sbin/kadmin.local -q "listprincs" > /dev/null 2>&1; do
    echo "  KDC not ready yet, retrying in 2s..."
    sleep 2
done
echo "KDC is ready."

IMPALA_KEYTAB_PATH="${KEYTAB_DIR}/impala.keytab"

# Create principals with explicit AES enctypes (compatible with modern MIT krb5 clients)
/usr/sbin/kadmin.local -q "addprinc -randkey -e aes256-cts-hmac-sha1-96:normal,aes128-cts-hmac-sha1-96:normal ${BACKEND_PRINCIPAL}" 2>/dev/null || true
/usr/sbin/kadmin.local -q "addprinc -randkey -e aes256-cts-hmac-sha1-96:normal,aes128-cts-hmac-sha1-96:normal ${IMPALA_PRINCIPAL}" 2>/dev/null || true

# Export keytab (only AES keys)
mkdir -p "${KEYTAB_DIR}"
/usr/sbin/kadmin.local -q "ktadd -k ${KEYTAB_PATH} -e aes256-cts-hmac-sha1-96:normal,aes128-cts-hmac-sha1-96:normal ${BACKEND_PRINCIPAL}"
chmod 600 "${KEYTAB_PATH}"
echo "Keytab exported to ${KEYTAB_PATH}"

/usr/sbin/kadmin.local -q "ktadd -k ${IMPALA_KEYTAB_PATH} -e aes256-cts-hmac-sha1-96:normal,aes128-cts-hmac-sha1-96:normal ${IMPALA_PRINCIPAL}"
chmod 644 "${IMPALA_KEYTAB_PATH}"
echo "Impala keytab exported to ${IMPALA_KEYTAB_PATH}"

# Keep the KDC running in the foreground
wait
