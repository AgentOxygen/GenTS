FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/gents

RUN pip install --upgrade pip
RUN pip install pytest asv sphinx sphinx-autobuild

COPY . .

RUN pip install -e .[parallel]

RUN git config --global --add safe.directory /project/.git

EXPOSE 8000

CMD ["pytest", "-v", "gents/tests/"]