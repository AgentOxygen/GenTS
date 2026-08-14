#!/bin/sh
# Downloads a case archive built by `gents_conform_build`, runs GenTS over it
# with --no-data, then checks the result with `gents_conform`. See the
# Dockerfile in this directory for how this is invoked.
set -eu

usage() {
    echo "Usage: docker run --rm [-v host_dir:/output] <image> <archive-url> <model>" >&2
    echo "  archive-url  URL of a .tar.xz case clone built by gents_conform_build" >&2
    echo "  model        Model spec passed to both run_gents and gents_conform (e.g. CESM3)" >&2
    exit 1
}

[ $# -eq 2 ] || usage

URL=$1
MODEL=$2

SAMPLE_DIR=/sample
OUTPUT_DIR=/output
ARCHIVE=/tmp/case.tar.xz

mkdir -p "$SAMPLE_DIR" "$OUTPUT_DIR"

echo "Downloading case archive from $URL"
curl -fsSL "$URL" -o "$ARCHIVE"

echo "Extracting to $SAMPLE_DIR"
tar -xf "$ARCHIVE" -C "$SAMPLE_DIR"

CMD_TXT=$(find "$SAMPLE_DIR" -maxdepth 3 -name cmd.txt -print -quit)
if [ -z "$CMD_TXT" ]; then
    echo "error: no cmd.txt found under $SAMPLE_DIR; '$URL' does not look like a gents_conform_build case clone" >&2
    exit 1
fi
CASE_DIR=$(dirname "$CMD_TXT")

echo "Case directory : $CASE_DIR"
echo "Model          : $MODEL"

run_gents "$CASE_DIR" -o "$OUTPUT_DIR" -nd -hc "${HFCORES:-4}" -tc "${TSCORES:-4}" -m "$MODEL"
gents_conform "$OUTPUT_DIR" -i "$CASE_DIR" -m "$MODEL" --json "$OUTPUT_DIR/report.json"
