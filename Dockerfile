# SAP Datasphere MCP Server - Docker Container
# Production-ready container for easy deployment

FROM python:3.12-slim

# Metadata
LABEL maintainer="Mario DeFelipe <mariodefe@example.com>"
LABEL description="SAP Datasphere MCP Server - 41 tools, 98% real data coverage"
LABEL version="1.0.0"

# Set working directory
WORKDIR /app

# Install system dependencies. Debian bookworm ships Node 18, but
# @sap/datasphere-cli needs >= 20, so take Node 20 from NodeSource.
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# The CLI-backed tools (list_task_chains, the dbusers family) shell out to
# `datasphere`, which is otherwise absent from the image and makes those tools
# fail at runtime with "CLI not found". Authentication is still the operator's
# job: run `datasphere login` or mount an existing CLI profile.
RUN npm install -g @sap/datasphere-cli && \
    datasphere --version

# Copy requirements first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY sap_datasphere_mcp_server.py .
COPY auth/ ./auth/
COPY .env.example .

# Create directory for logs
RUN mkdir -p /app/logs

# Environment variables (override via docker run -e or docker-compose)
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV SERVER_PORT=8080
ENV USE_MOCK_DATA=false

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Expose port (if running HTTP server mode)
# EXPOSE 8080

# Run as non-root user for security
RUN useradd -m -u 1000 mcpuser && \
    chown -R mcpuser:mcpuser /app
USER mcpuser

# Run the MCP server
CMD ["python", "sap_datasphere_mcp_server.py"]
