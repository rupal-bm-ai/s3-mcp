"""
S3 MCP Server — using FastMCP (handles SSE routing automatically)
Hosted on Railway, callable by Cursor Automations as a remote MCP server.
"""

import gzip
import logging
import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("s3-mcp-server")

AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# FastMCP handles all SSE transport and routing automatically
mcp = FastMCP("s3-mcp-server", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))



# ── S3 client ─────────────────────────────────────────────────────────────────

def s3_client(region: str = AWS_REGION):
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def read_file(bucket: str, key: str, region: str = AWS_REGION) -> str:
    """
    Fetch a file from S3. Automatically decompresses .gz files.
    Returns the last 500 lines for large log files.
    Returns NOT_FOUND if the key does not exist.
    """
    s3 = s3_client(region)
    logger.info("Fetching s3://%s/%s", bucket, key)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        raw = response["Body"].read()
    except NoCredentialsError:
        return "ERROR: AWS credentials not configured on the server."
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return f"NOT_FOUND: s3://{bucket}/{key}"
        return f"S3 error [{code}]: {e.response['Error']['Message']}"
    except Exception as e:
        logger.exception("Unexpected error in read_file")
        return f"Unexpected error: {e}"

    # Auto-decompress gzip
    if key.endswith(".gz"):
        try:
            raw = gzip.decompress(raw)
        except Exception as e:
            return f"Could not decompress .gz file: {e}"

    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()

    truncated_note = ""
    if len(lines) > 500:
        truncated_note = f"[Truncated to last 500 of {len(lines)} lines]\n\n"
        lines = lines[-500:]

    return truncated_note + "\n".join(lines)


@mcp.tool()
def list_objects(bucket: str, prefix: str, region: str = AWS_REGION, max_keys: int = 100) -> str:
    """
    List objects in an S3 bucket under a given prefix.
    Returns a newline-separated list of s3:// paths with sizes.
    """
    s3 = s3_client(region)
    logger.info("Listing s3://%s/%s", bucket, prefix)

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    except NoCredentialsError:
        return "ERROR: AWS credentials not configured on the server."
    except ClientError as e:
        return f"S3 error [{e.response['Error']['Code']}]: {e.response['Error']['Message']}"
    except Exception as e:
        logger.exception("Unexpected error in list_objects")
        return f"Unexpected error: {e}"

    contents = response.get("Contents", [])
    if not contents:
        return f"No objects found under s3://{bucket}/{prefix}"

    lines = [f"s3://{bucket}/{obj['Key']}  ({obj['Size']} bytes)" for obj in contents]
    return "\n".join(lines)


@mcp.tool()
def object_exists(bucket: str, key: str, region: str = AWS_REGION) -> str:
    """
    Check whether an S3 object exists without downloading it.
    Returns EXISTS or NOT_FOUND.
    """
    s3 = s3_client(region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return "EXISTS"
    except NoCredentialsError:
        return "ERROR: AWS credentials not configured on the server."
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return "NOT_FOUND"
        return f"S3 error [{e.response['Error']['Code']}]: {e.response['Error']['Message']}"
    except Exception as e:
        logger.exception("Unexpected error in object_exists")
        return f"Unexpected error: {e}"


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # transport="sse" exposes /sse and /messages/ automatically
    mcp.run(transport="sse")
