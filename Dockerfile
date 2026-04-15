ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim AS base

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for better caching
COPY pyproject.toml ./
COPY README.md .
COPY src/ src/

# Install the package
RUN pip install --no-cache-dir ".[postgres,redis]"

# Production image
FROM python:${PYTHON_VERSION}-slim AS production

WORKDIR /app

COPY --from=base /usr/local/lib/ /usr/local/lib/
COPY --from=base /usr/local/bin/gravtory /usr/local/bin/gravtory
COPY --from=base /app /app

# Non-root user
RUN useradd --create-home gravtory
USER gravtory

ENV GRAVTORY_BACKEND=sqlite:///data/gravtory.db

EXPOSE 7777

ENTRYPOINT ["gravtory"]
CMD ["--help"]
