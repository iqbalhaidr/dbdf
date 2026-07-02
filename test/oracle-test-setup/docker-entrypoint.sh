#!/bin/sh
set -e

# Path instalasi Oracle Instant Client tergantung versi yang di-resolve
# otomatis oleh repo oracle-instantclient-release-el8, jadi dicari dinamis
# di sini alih-alih di-hardcode lewat ENV saat build.
ORACLE_LIB_DIR=$(dirname "$(find /usr /opt -name 'libclntsh.so*' 2>/dev/null | head -n1)")
ORACLE_BIN_DIR=$(dirname "$(find /usr /opt -name 'sqlldr' 2>/dev/null | head -n1)")

if [ -z "$ORACLE_LIB_DIR" ] || [ "$ORACLE_LIB_DIR" = "." ]; then
    echo "ERROR: libclntsh.so (Oracle Instant Client) tidak ditemukan." >&2
    exit 1
fi
if [ -z "$ORACLE_BIN_DIR" ] || [ "$ORACLE_BIN_DIR" = "." ]; then
    echo "ERROR: sqlldr tidak ditemukan." >&2
    exit 1
fi

export LD_LIBRARY_PATH="$ORACLE_LIB_DIR:$LD_LIBRARY_PATH"
export PATH="$ORACLE_BIN_DIR:$PATH"

exec "$@"
