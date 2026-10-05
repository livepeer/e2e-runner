FROM golang:1.27.1-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git python3 python3-venv build-essential pkg-config \
    nasm libx264-dev libgnutls28-dev libsqlite3-dev docker.io \
    && rm -rf /var/lib/apt/lists/*
# CPU-only media libraries suffice for runner protocol and media round trips.
RUN git clone --depth 1 --branch n8.1.2 https://github.com/FFmpeg/FFmpeg.git /ffmpeg \
    && cd /ffmpeg && ./configure --enable-gpl --enable-libx264 --enable-gnutls \
       --enable-shared --disable-static --disable-doc --disable-debug \
    && make -j4 && make install && ldconfig && rm -rf /ffmpeg
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /usr/local/bin/uv
COPY --from=docker:28.5.2-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker:28.5.2-cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
ENV UV_PYTHON_DOWNLOADS=automatic UV_LINK_MODE=copy GOTOOLCHAIN=auto
