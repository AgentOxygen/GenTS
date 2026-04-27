FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /usr/local/gents
COPY . .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/gents-env"

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    git config --global --add safe.directory /usr/local/gents && \
    git config --global --add safe.directory /usr/local/gents/.git

RUN uv venv $UV_PROJECT_ENVIRONMENT && \
    uv pip install --python $UV_PROJECT_ENVIRONMENT -r requirements.txt && \
    uv pip install --python $UV_PROJECT_ENVIRONMENT -e .

ENV PATH="/usr/local/gents-env/bin:$PATH"
CMD ["pytest", "-v", "gents/tests/"]
