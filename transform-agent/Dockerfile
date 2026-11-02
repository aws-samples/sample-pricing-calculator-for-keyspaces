# Canonical AWS Transform orchestrator Dockerfile (aws-transform-agent-toolkit
# steering/dockerfile-orchestrator.md), plus Node.js and the bundled Keyspaces
# calculator. Run ./build.sh first so vendor/ and skills/ exist in the build context.
FROM --platform=linux/arm64 public.ecr.aws/docker/library/python:3.11-slim

WORKDIR /app

# Install system dependencies (nodejs runs the bundled Keyspaces calculator CLIs)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl nodejs && \
    rm -rf /var/lib/apt/lists/*

# Install AWS Transform SDK from PyPI
RUN pip install --no-cache-dir \
    'agent-builder-sdk-aws-transform>=1.0.0' \
    'agent-builder-agentic-mcp-aws-transform>=1.0.0' \
    'agent-builder-types-aws-transform>=1.0.0' \
    'agent-builder-mcp-client-aws-transform>=1.0.0'

# Register botocore service models (REQUIRED for Agentic API and Agent Registry API)
RUN pip install --no-cache-dir awscli && \
    SDK_MODELS=$(python -c "from importlib.resources import files; print(files('agent_builder_sdk').joinpath('botocore_models'))") && \
    aws configure add-model --service-name atxagentregistryexternal \
      --service-model "file://${SDK_MODELS}/atxagentregistryexternal/2022-07-26/service-2.json" && \
    aws configure add-model --service-name transformagenticservice \
      --service-model "file://${SDK_MODELS}/transformagenticservice/2018-05-10/service-2.json"

# Install remaining Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create MCP server wrapper binary
RUN mkdir -p /home/amazon/AgentBuilderAgenticMCP/bin && \
    printf '#!/bin/bash\npython -m agent_builder_agentic_mcp "$@"\n' > /home/amazon/AgentBuilderAgenticMCP/bin/agent-builder-agentic-mcp && \
    chmod +x /home/amazon/AgentBuilderAgenticMCP/bin/agent-builder-agentic-mcp

# Copy orchestrator source, bundled calculator, and the Keyspaces skill
COPY src/ .
COPY vendor/ /app/vendor/
COPY skills/ /app/skills/

ENV KEYSPACES_VENDOR_DIR=/app/vendor/calculator \
    KEYSPACES_SKILLS_DIR=/app/skills \
    KEYSPACES_WORK_DIR=/tmp/keyspaces_work

# Create storage directories
RUN mkdir -p /tmp/agent_queue /tmp/orchestrator_agent /tmp/keyspaces_work

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/ping || exit 1

ENTRYPOINT ["python", "app.py"]
