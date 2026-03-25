FROM python:3.10.7-slim as base


ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME="/opt/poetry"

ENV PATH="$POETRY_HOME/bin:$PATH"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml poetry.lock ./
RUN useradd -m archangel

FROM base AS api-image
RUN poetry install --no-root --only main,api --no-interaction --no-ansi
COPY . /app/backend
ENV PYTHONPATH=/app
RUN chown -R archangel /app
USER archangel

FROM base AS consumer-image
RUN poetry install --no-root --only main,consumer --no-interaction --no-ansi
COPY . /app/backend
ENV PYTHONPATH=/app
RUN chown -R archangel /app
USER archangel

FROM base AS init_db-image
RUN poetry install --no-root --only main,init_db --no-interaction --no-ansi
COPY . /app/backend
ENV PYTHONPATH=/app
RUN chown -R archangel /app
USER archangel
