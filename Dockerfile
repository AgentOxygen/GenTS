FROM python:3.11-slim

WORKDIR /project

COPY pyproject.toml requirements.txt ./

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install pytest

COPY . .

RUN pip install -e .

CMD ["pytest", "-v", "gents/tests/"]