"""
S3 MCP Server — HTTP/SSE transport
Hosted on Railway, callable by Cursor Automations as a remote MCP server.

Transport: MCP over SSE (Server-Sent Events) via FastAPI
"""

import gzip
import logging
import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("s3-mcp-server")

AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# ── S3 helpers ────────────────────────────────────────────────────────────────

def s3_client(region: str = AWS_REGION):
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


# ── MCP Server ────────────────────────────────────────────────────────────────

mcp = Server("s3-mcp-server")


@mcp.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="read_file",
            description=(
                "Fetch a file from S3. Automatically decompresses .gz files. "
                "Returns the last 500 lines for large log files."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {"type": "string", "description": "S3 bucket name"},
                    "key":    {"type": "string", "description": "Full S3 object key"},
                    "region": {"type": "string", "description": "AWS region (default: us-west-2)"},
                },
                "required": ["bucket", "key"],
            },
        ),
        Tool(
            name="list_objects",
            description="List objects in an S3 bucket under a given prefix.",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket":   {"type": "string", "description": "S3 bucket name"},
                    "prefix":   {"type": "string", "description": "Key prefix to filter by"},
                    "region":   {"type": "string", "description": "AWS region (default: us-west-2)"},
                    "max_keys": {"type": "integer", "description": "Max results (default 100)"},
                },
                "required": ["bucket", "prefix"],
            },
        ),
        Tool(
            name="object_exists",
            description="Check whether an S3 object exists without downloading it.",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "key":    {"type": "string"},
                    "region": {"type": "string"},
                },
                "required": ["bucket", "key"],
            },
        ),
    ]


@mcp.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if name == "read_file":
            return await _read_file(arguments)
        elif name == "list_objects":
            return await _list_objects(arguments)
        elif name == "object_exists":
            return await _object_exists(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except NoCredentialsError:
        return [TextContent(type="text", text="ERROR: AWS credentials not configured on the server.")]
    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg  = e.response["Error"]["Message"]
        return [TextContent(type="text", text=f"S3 error [{code}]: {msg}")]
    except Exception as e:
        logger.exception("Unexpected error in tool %s", name)
        return [TextContent(type="text", text=f"Unexpected error: {e}")]


async def _read_file(args: dict) -> list[TextContent]:
    bucket = args["bucket"]
    key    = args["key"]
    region = args.get("region", AWS_REGION)

    s3 = s3_client(region)
    logger.info("Fetching s3://%s/%s", bucket, key)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        raw      = response["Body"].read()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return [TextContent(type="text", text=f"NOT_FOUND: s3://{bucket}/{key}")]
        raise

    if key.endswith(".gz"):
        try:
            raw = gzip.decompress(raw)
        except Exception as e:
            return [TextContent(type="text", text=f"Could not decompress .gz: {e}")]

    text  = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()

    truncated_note = ""
    if len(lines) > 500:
        truncated_note = f"[Truncated to last 500 of {len(lines)} lines]\n\n"
        lines = lines[-500:]

    return [TextContent(type="text", text=truncated_note + "\n".join(lines))]


async def _list_objects(args: dict) -> list[TextContent]:
    bucket   = args["bucket"]
    prefix   = args.get("prefix", "")
    region   = args.get("region", AWS_REGION)
    max_keys = int(args.get("max_keys", 100))

    s3 = s3_client(region)
    logger.info("Listing s3://%s/%s", bucket, prefix)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    contents = response.get("Contents", [])

    if not contents:
        return [TextContent(type="text", text=f"No objects found under s3://{bucket}/{prefix}")]

    lines = [f"s3://{bucket}/{obj['Key']}  ({obj['Size']} bytes)" for obj in contents]
    return [TextContent(type="text", text="\n".join(lines))]


async def _object_exists(args: dict) -> list[TextContent]:
    bucket = args["bucket"]
    key    = args["key"]
    region = args.get("region", AWS_REGION)

    s3 = s3_client(region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return [TextContent(type="text", text="EXISTS")]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return [TextContent(type="text", text="NOT_FOUND")]
        raise


# ── SSE Transport + Starlette app ─────────────────────────────────────────────

sse = SseServerTransport("/messages/")


async def handle_sse(request: Request):
    async with sse.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp.run(
            streams[0], streams[1], mcp.create_initialization_options()
        )


async def health(request: Request):
    return JSONResponse({"status": "ok", "server": "s3-mcp-server"})


app = Starlette(
    routes=[
        Route("/health", health),
        Route("/sse",    handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ]
)
