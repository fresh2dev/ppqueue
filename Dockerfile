ARG IMAGE_REGISTRY=docker.io
FROM ${IMAGE_REGISTRY}/nginx:1

COPY public /usr/share/nginx/html
