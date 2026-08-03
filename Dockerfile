ARG DEFAULT_PYTHON=3.12
ARG FLOOR_PYTHON=3.10
ARG LATEST_PYTHON=3.14

FROM ghcr.io/astral-sh/uv:python${DEFAULT_PYTHON}-trixie-slim AS runtime
ENV HDF5_USE_FILE_LOCKING=FALSE
RUN apt-get update && apt-get install -y --no-install-recommends git
WORKDIR /usr/local/gents
COPY . .
RUN uv pip install --system .
USER 1000:1000
CMD ["run_gents"]

FROM runtime AS test
USER root
RUN uv pip install --system -e ".[test]"
USER 1000:1000
CMD ["pytest", "-v", "gents/tests/"]

FROM runtime AS bench
ENV HOME=/usr/local/gents
USER root
RUN rm -rf /var/lib/apt/lists/* && git config --system --add safe.directory '*'
RUN uv pip install --system ".[bench]"
USER 1000:1000
CMD ["asv", "run"]

FROM bench AS dev
USER root
RUN uv pip install --system -e ".[dev]"
USER 1000:1000
CMD ["bash"]

FROM ghcr.io/astral-sh/uv:python${FLOOR_PYTHON}-trixie-slim AS deptest-floor
RUN apt-get update && apt-get install -y --no-install-recommends git
COPY . .
RUN uv pip install --system --resolution lowest-direct . \
 && uv pip install --system pytest \
 && pytest -v gents/tests/

FROM ghcr.io/astral-sh/uv:python${LATEST_PYTHON}-trixie-slim AS deptest-latest
RUN apt-get update && apt-get install -y --no-install-recommends git
COPY . .
ARG CACHEBUST=unset
RUN uv pip install --system --upgrade . \
 && uv pip install --system pytest \
 && pytest -v gents/tests/