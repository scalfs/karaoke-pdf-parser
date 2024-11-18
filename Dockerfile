FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jre \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first
COPY requirements.txt .

# Clean pip cache and install dependencies
RUN pip cache purge && \
    pip install --no-cache-dir numpy==1.23.5 && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY pdf_parser.py .

# Create directory for logs
RUN mkdir -p /app/logs

# Volume for persistent storage
VOLUME ["/app/logs", "/app/data"]

# Command to run the script
CMD ["python", "pdf_parser.py"]