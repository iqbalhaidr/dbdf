FROM oraclelinux:8-slim

# "oracle-instantclient-release-el8" adalah paket yang benar untuk mendaftarkan
# repo Instant Client (bukan "oracle-release-el8", yang cuma repo OS umum).
# Versi tidak di-pin (mis. 19.19) karena bisa saja tidak tersedia persis di
# repo -- pakai nama paket tanpa versi supaya ambil versi terbaru yang ada.
RUN microdnf install -y oracle-instantclient-release-el8 && \
    microdnf install -y oracle-instantclient-basic oracle-instantclient-tools && \
    microdnf install -y python39 python39-pip findutils && \
    microdnf clean all

# Pastikan sqlldr benar-benar terpasang (build gagal kalau tidak ada)
RUN find /usr /opt -name 'sqlldr' 2>/dev/null | grep -q sqlldr

WORKDIR /workspace

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

COPY . .

# Entrypoint mendeteksi lokasi Instant Client secara dinamis lalu set
# LD_LIBRARY_PATH/PATH sebelum menjalankan perintah (lihat docker-entrypoint.sh)
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python3", "test_write_database.py"]

