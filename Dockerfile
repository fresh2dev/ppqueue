ARG HB_IMAGE_REGISTRY=docker.io
FROM ${HB_IMAGE_REGISTRY}/python:3.10.10-slim-bullseye as build
LABEL org.opencontainers.image.source=https://www.github.com/fresh2dev/ppqueue
LABEL org.opencontainers.image.description="A Parallel Process Queue for Python."
LABEL org.opencontainers.image.licenses=MIT
RUN apt-get update && apt-get install --upgrade -y build-essential git
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"
RUN python3 -m pip install --no-cache-dir --upgrade pip
COPY ./dist /dist
RUN find /dist -name "*.whl" -exec \
    pip install --no-cache-dir \
        --extra-index-url "https://codeberg.org/api/packages/Fresh2dev/pypi/simple" \
        "{}" \; \
    && pip show "ppqueue"

FROM ${HB_IMAGE_REGISTRY}/python:3.10.10-slim-bullseye
COPY --from=build /app/venv /app/venv
COPY --from=build /usr/local/bin /usr/local/bin
ENV PATH="/app/venv/bin:$PATH"
ENTRYPOINT ["ppqueue"]
WORKDIR /workspace
