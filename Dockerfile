FROM python:3.11-slim

WORKDIR /project

COPY pyproject.toml requirements.txt ./

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install pytest sphinx sphinx-autobuild

COPY . .

RUN pip install -e .

EXPOSE 8000

CMD ["pytest", "-v", "gents/tests/"]