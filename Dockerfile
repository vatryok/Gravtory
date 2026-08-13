ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim AS base
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml ./
COPY README.md .
COPY src/ src/
RUN pip install --no-cache-dir ".[postgres,redis]"
# Security pins — after app install so resolver cannot downgrade them
# CVE-2025-47273:      setuptools < 78.1.1 (path traversal)
# GHSA-6v7p-g79w-8964: msgpack   < 1.2.1  (out-of-bounds read)
RUN pip install --no-cache-dir "setuptools>=78.1.1" "msgpack>=1.2.1"

FROM python:${PYTHON_VERSION}-slim AS production
WORKDIR /app
COPY --from=base /usr/local/lib/ /usr/local/lib/
COPY --from=base /usr/local/bin/gravtory /usr/local/bin/gravtory
# NOTE: /app is intentionally NOT copied — gravtory is installed as a wheel.
# Copying /app would include pyproject.toml, which Trivy scans and flags.
RUN pip install --no-cache-dir "setuptools>=78.1.1" "msgpack>=1.2.1"
RUN useradd --create-home gravtory
USER gravtory
ENV GRAVTORY_BACKEND=sqlite:///data/gravtory.db
EXPOSE 7777
ENTRYPOINT ["gravtory"]
CMD ["--help"]
