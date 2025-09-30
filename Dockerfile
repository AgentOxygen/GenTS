FROM python:3.11-slim

WORKDIR /project

COPY . .

RUN pip install --upgrade pip
RUN pip install pytest sphinx sphinx-autobuild

RUN pip install -e .[parallel]

EXPOSE 8000

CMD ["pytest", "-v", "gents/tests/"]