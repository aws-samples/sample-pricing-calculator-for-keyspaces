"""AWS Transform entry point for the Keyspaces Migration orchestrator."""

import argparse
import logging
import os
from pathlib import Path

from agent_builder_sdk.server.agent_runtime_server import AgentRuntimeServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

_HERE = Path(__file__).resolve().parent
PROMPT_PATH = _HERE / "prompts" / "orchestrator_prompt.md"
# Local default: transform-agent/skills (populated by build.sh). The Dockerfile sets KEYSPACES_SKILLS_DIR.
SKILLS_DIR = os.environ.get("KEYSPACES_SKILLS_DIR", str(_HERE.parent / "skills"))
# Bedrock requires a cross-Region inference profile ID (us. prefix); bare model IDs fail.
MODEL_ID = os.environ.get("KEYSPACES_AGENT_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keyspaces Migration (AWS Transform agent)")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind server to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind server to")
    parser.add_argument("--storage-dir", default="/tmp/orchestrator_agent", help="Agent data storage directory")
    parser.add_argument(
        "--binary-location",
        default="/home/amazon/AgentBuilderAgenticMCP/bin/agent-builder-agentic-mcp",
        help="Path to the agentic MCP server binary",
    )
    return parser


def agent_factory(mcp_client, storage_dir=None):
    # Imports stay inside the factory: module-level SDK agent construction hangs in AgentCore containers.
    from agent_builder_sdk.agent_factory import create_default_async_orchestrator_with_subagent

    from keyspaces_tools import KEYSPACES_TOOLS

    return create_default_async_orchestrator_with_subagent(
        system_prompt=PROMPT_PATH.read_text(),
        mcp_client=mcp_client,
        storage_dir=storage_dir or "/tmp/orchestrator_agent",
        custom_tools=KEYSPACES_TOOLS,
        model_id=MODEL_ID,
        with_base_guardrails=True,
        enable_skills=True,
        skills_dir=SKILLS_DIR,
    )


def main():
    args = create_parser().parse_args()
    logger.info("Starting Keyspaces Migration (model=%s, skills=%s)", MODEL_ID, SKILLS_DIR)
    server = AgentRuntimeServer(
        agent_factory=agent_factory,
        host=args.host,
        port=args.port,
        binary_location=args.binary_location,
        storage_dir=args.storage_dir,
        delayed_timeout=3600,
    )
    server.start()


if __name__ == "__main__":
    main()
