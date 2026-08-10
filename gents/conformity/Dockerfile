# Conformity test image. Downloads an archived case clone named at `docker
# run` time, runs GenTS over it with --no-data, then checks the result
# against a model specification. One image serves every archived case and
# every model -- adding a case is an upload, not an image rebuild.
#
# The build context must be the repository root, because the image installs
# GenTS from source:
#
#   docker build -f gents/conformity/Dockerfile -t gents-conformity .
#   docker run --rm gents-conformity <case-archive-url> <model>
#
# e.g.
#   docker run --rm gents-conformity \
#     https://pub-32310187d5784979970f4bf1871d10b2.r2.dev/b.e30_alpha09d_m.B1850C_MTso_Gris.ne30_t233_wgx3.369.tar.xz \
#     CESM3
#
# Add -v "$PWD/out:/output" to keep the generated time series and JSON report.
ARG DEFAULT_PYTHON=3.12
FROM ghcr.io/astral-sh/uv:python${DEFAULT_PYTHON}-trixie-slim

ENV HDF5_USE_FILE_LOCKING=FALSE

# git: setuptools-scm derives the version from the repository.
# curl: fetches the case archive named in `docker run`'s arguments.
# xz-utils: tar has no built-in xz support, unlike Docker's own ADD/COPY.
RUN apt-get update && apt-get install -y --no-install-recommends git curl xz-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/gents
COPY . .
RUN uv pip install --system . \
    && chmod +x gents/conformity/run_case.sh

ENTRYPOINT ["/usr/local/gents/gents/conformity/run_case.sh"]
