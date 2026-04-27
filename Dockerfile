FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /usr/local/gents
COPY . .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/gents-env"

RUN uv venv $UV_PROJECT_ENVIRONMENT && \
    uv pip install --python $UV_PROJECT_ENVIRONMENT -r requirements.txt && \
    uv pip install --python $UV_PROJECT_ENVIRONMENT -e .

ENV PATH="/usr/local/gents-env/bin:$PATH"
CMD ["pytest", "-v", "gents/tests/"]
