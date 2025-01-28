# https://docs.astral.sh/uv/guides/integration/docker/#available-images
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

COPY pyproject.toml /pyproject.toml
COPY uv.lock /uv.lock

# Synchronize dependencies defined in pyproject.toml
RUN uv sync

# Set the working directory inside the container
WORKDIR /fetch-exercise

RUN apt-get -y update && apt-get -y install curl
RUN curl -O https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh
RUN chmod +x wait-for-it.sh

CMD ["uv", "run", "/fetch-exercise/scripts/load_db.py"]