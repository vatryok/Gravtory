ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim AS base
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*
# Fix PYSEC-2026-3447 / CVE-2025-47273: python:slim ships setuptools 70.3.0
RUN pip install --no-cache-dir "setuptools==83.0.0"
COPY pyproject.toml ./
COPY README.md .
COPY src/ src/
RUN pip install --no-cache-dir ".[postgres,redis]"

FROM python:${PYTHON_VERSION}-slim AS production
WORKDIR /app
COPY --from=base /usr/local/lib/ /usr/local/lib/
COPY --from=base /usr/local/bin/gravtory /usr/local/bin/gravtory
COPY --from=base /app /app
# COPY does not remove old dist-info from base python image layer — re-pin explicitly
RUN pip install --no-cache-dir "setuptools==83.0.0"
RUN useradd --create-home gravtory
USER gravtory
ENV GRAVTORY_BACKEND=sqlite:///data/gravtory.db
EXPOSE 7777
ENTRYPOINT ["gravtory"]
CMD ["--help"]
