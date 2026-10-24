FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.13.4 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable && useradd --create-home --uid 10001 retail \
    && mkdir /app/data && chown retail:retail /app/data
USER 10001:10001
ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["retail"]
CMD ["--help"]
