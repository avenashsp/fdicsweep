#!/usr/bin/env bash
# =============================================================================
# sweep_stream.sh : Zero-disk, zero-python streaming pipe from S3 to STDOUT.
# Usage: sweep_stream.sh <s3_uri_or_path> [aws_region]
#
# Rules:
# 1. Takes only GZIP files (*.gz), decompresses and streams to stdout.
# 2. If given a plain CSV file (*.csv), streams directly to stdout.
# 3. set -o pipefail ensures any S3 download error or corrupted gzip aborts
#    the pipe immediately with non-zero exit code, forcing psql transaction rollback.
# =============================================================================
set -o pipefail

# Ensure PYTHONPATH includes system and local site-packages so aws cli finds dateutil
export PYTHONPATH="/usr/local/lib/python3.9/site-packages:/usr/local/lib64/python3.9/site-packages:/usr/lib/python3.9/site-packages:$PYTHONPATH"

SRC="$1"
REGION="${2:-${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-2}}}"

if [ -z "$SRC" ]; then
  echo "Error: No source URI or path specified to sweep_stream.sh" >&2
  exit 1
fi

case "$SRC" in
  s3://*)
    case "$SRC" in
      *.gz|*.GZ)
        aws s3 cp "$SRC" - --region "$REGION" --only-show-errors | gzip -dc
        ;;
      *)
        aws s3 cp "$SRC" - --region "$REGION" --only-show-errors
        ;;
    esac
    ;;
  *.gz|*.GZ)
    gzip -dc "$SRC"
    ;;
  *)
    cat "$SRC"
    ;;
esac
