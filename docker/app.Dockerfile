# l2tape Python services (snapshot, health). Build context: repo root.
FROM python:3.12-slim-trixie
COPY --from=astral/uv:0.13.5 /uv /bin/uv
ENV UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev
ENV PATH=/app/.venv/bin:$PATH
# Run directly, not via `uv run`: the process gets the stop signal itself.
ENTRYPOINT ["l2tape"]
