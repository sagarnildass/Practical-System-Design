FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install MongoDB
RUN wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - \
    && echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/6.0 main" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list \
    && apt-get update \
    && apt-get install -y mongodb-org \
    && mkdir -p /data/db \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Redis
RUN apt-get update && apt-get install -y --no-install-recommends \
    redis-server \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the crawler code
COPY . .

# Create necessary directories
RUN mkdir -p /data/storage/html_pages \
    && mkdir -p /data/storage/logs \
    && mkdir -p /data/storage/exports

# Expose ports
# Prometheus metrics port
EXPOSE 9100
# MongoDB port
EXPOSE 27017
# Redis port
EXPOSE 6379

# Set environment variables
ENV MONGODB_URI=mongodb://localhost:27017/
ENV REDIS_URI=redis://localhost:6379/0
ENV PYTHONUNBUFFERED=1

# Create entrypoint script
RUN echo '#!/bin/bash\n\
# Start MongoDB\n\
mongod --fork --logpath /var/log/mongodb.log\n\
\n\
# Start Redis\n\
redis-server --daemonize yes\n\
\n\
# Check if services are running\n\
echo "Waiting for MongoDB to start..."\n\
until mongo --eval "print(\"MongoDB is ready\")" > /dev/null 2>&1; do\n\
    sleep 1\n\
done\n\
\n\
echo "Waiting for Redis to start..."\n\
until redis-cli ping > /dev/null 2>&1; do\n\
    sleep 1\n\
done\n\
\n\
echo "All services are running!"\n\
\n\
# Execute the provided command or default to help\n\
if [ $# -eq 0 ]; then\n\
    python crawl.py --help\n\
else\n\
    exec "$@"\n\
fi' > /app/entrypoint.sh \
    && chmod +x /app/entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command is to show help
CMD ["python", "crawl.py", "--help"] 