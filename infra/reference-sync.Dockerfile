# Built from the repository root: docker build -f infra/reference-sync.Dockerfile .
FROM python:3.12.11-slim
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .
ENTRYPOINT ["darwin-reference-sync"]
