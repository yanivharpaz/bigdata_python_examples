FROM python:3.11-slim

# Install Kerberos system packages needed for kinit and GSSAPI
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-user \
    libkrb5-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app/ ./app/
COPY docker/backend-entrypoint.sh /backend-entrypoint.sh
RUN chmod +x /backend-entrypoint.sh

# Point Kerberos client at the mounted krb5.conf (overridden at runtime by entrypoint)
ENV KRB5_CONFIG=/etc/krb5/krb5.conf

EXPOSE 8000

ENTRYPOINT ["/backend-entrypoint.sh"]
