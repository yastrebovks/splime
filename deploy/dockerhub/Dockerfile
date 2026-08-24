# syntax=docker/dockerfile:1
# check=skip=SecretsUsedInArgOrEnv
# (SPL_DAEMON_SECRET_BACKEND selects the secret backend *type*, not a secret value.)

# ---------------------------------------------------------------------------
# splime daemon - public image for Docker Hub (yastrebovks/spl-daemon)
#
# Installs the released `splime` package from PyPI, so the build context is
# tiny and needs no repository source. Runs the local SPL daemon as a non-root
# user. The daemon binds 0.0.0.0 INSIDE the container only - publish it on host
# loopback (127.0.0.1) and authenticate with the bearer token written to
# daemon-endpoint.json.
#
#   docker build -t yastrebovks/spl-daemon:<version> .
#   docker run -d -p 127.0.0.1:8765:8765 \
#       -v /var/lib/spl-daemon:/var/lib/spl-daemon yastrebovks/spl-daemon:<version>
#
# Both external source images are pinned to reviewed multi-arch digests.
# ---------------------------------------------------------------------------
FROM docker:27-cli@sha256:851f91d241214e7c6db86513b270d58776379aacc5eb9c4a87e5b47115e3065c AS docker-cli
FROM python:3.13-slim@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a

# Static, multi-arch Docker CLI for opt-in docker-runtime nodes (DooD). The
# image does NOT mount a Docker socket by default; that is an explicit opt-in.
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

# Which splime release to install (override with --build-arg SPL_VERSION=...).
ARG SPL_VERSION=0.4.8
ARG UV_VERSION=0.11.25
# Filled with the exact signed package-release commit by the separate,
# post-PyPI Docker release-control commit. The value below is deliberately
# non-publishable and prevents this source-preparation tree from claiming
# artifact provenance that does not exist yet.
ARG SPL_PACKAGE_REVISION=unpublished
ARG SPL_SOURCE_REVISION=unknown
ARG SPL_BUILD_DATE=unknown
ARG SPL_UID=10001
ARG SPL_GID=10001

LABEL org.opencontainers.image.title="splime daemon" \
      org.opencontainers.image.description="Local splime daemon: a private registry and execution layer for versioned, portable Python function nodes." \
      org.opencontainers.image.version="${SPL_VERSION}" \
      org.opencontainers.image.revision="${SPL_SOURCE_REVISION}" \
      org.opencontainers.image.created="${SPL_BUILD_DATE}" \
      io.splime.package.revision="${SPL_PACKAGE_REVISION}" \
      org.opencontainers.image.source="https://github.com/yastrebovks/splime" \
      org.opencontainers.image.url="https://hub.docker.com/r/yastrebovks/spl-daemon" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/var/lib/spl-daemon

# No desktop keyring in a container: keep secrets in the daemon home directory.
ENV SPL_DAEMON_SECRET_BACKEND=file

# Install the exact released package from PyPI. uv enables the fast
# environment-build path introduced in splime 0.4.3
# (uv venv --relocatable + uv pip install --strict). Without it the daemon
# transparently falls back to stdlib venv + pip and doctor prints a hint.
RUN python -m pip install "splime==${SPL_VERSION}" "uv==${UV_VERSION}"

# Non-root runtime user. The daemon home is bind-mounted here (see compose);
# keep that directory writable by this identity.
RUN groupadd --gid "${SPL_GID}" spl \
    && useradd \
        --uid "${SPL_UID}" \
        --gid "${SPL_GID}" \
        --home-dir /var/lib/spl-daemon \
        --shell /usr/sbin/nologin \
        --no-create-home \
        spl \
    && mkdir -p /var/lib/spl-daemon \
    && chown -R spl:spl /var/lib/spl-daemon

WORKDIR /var/lib/spl-daemon
USER spl:spl

EXPOSE 8765

# Liveness: read the local bearer token from daemon-endpoint.json and hit /health.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import json, os, urllib.request; home=os.environ.get('SPL_DAEMON_HOME') or '/var/lib/spl-daemon'; token=json.load(open(os.path.join(home, 'daemon-endpoint.json')))['api_token']; req=urllib.request.Request('http://127.0.0.1:8765/health', headers={'Authorization': 'Bearer ' + token}); urllib.request.urlopen(req)" || exit 1

# --no-auto-port pins the container to 8765 so the published mapping is stable.
CMD ["spl-daemon", "serve", \
     "--host", "0.0.0.0", "--port", "8765", \
     "--home", "/var/lib/spl-daemon", "--no-auto-port"]
