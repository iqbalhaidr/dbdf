FROM oraclelinux:8-slim

# Daftarkan repo Instant Client
RUN microdnf install -y oracle-instantclient-release-el8 && \
    microdnf install -y oracle-instantclient-basic oracle-instantclient-tools && \
    # Ganti ke python3.11 (tidak butuh gcc)
    microdnf install -y python3.11 python3.11-pip findutils && \
    microdnf clean all

# Pastikan sqlldr benar-benar terpasang
RUN find /usr /opt -name 'sqlldr' 2>/dev/null | grep -q sqlldr

WORKDIR /workspace

COPY requirements.txt .
# Gunakan pip3.11 secara eksplisit untuk menghindari salah versi
RUN pip3.11 install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

COPY . .

ENTRYPOINT ["docker-entrypoint.sh"]
# Eksekusi dengan python3.11
CMD ["python3.11", "test_write_database.py"]