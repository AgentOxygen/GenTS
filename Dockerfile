FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /usr/local/gents
COPY . .

RUN uv venv && \
    uv pip install pytest asv sphinx sphinx-autobuild && \
    uv pip install -e .

ENV PATH="/usr/local/gents/.venv/bin/:$PATH"
CMD ["pytest", "-v", "gents/tests/"]