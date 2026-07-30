# splime daemon - Docker Hub image

Public container image for the splime local daemon, built from the released
[`splime`](https://pypi.org/project/splime/) package on PyPI.

- **Image:** `yastrebovks/spl-daemon`
- **Tags:** versioned 0.4-series releases and `latest`
- **Base:** `python:3.13-slim` (multi-arch: `linux/amd64`, `linux/arm64`)
- **Runs as:** non-root user `spl` (`10001:10001`)
- **Port:** `8765` (bind to host loopback only)

The image installs `splime` from PyPI, so the build context is empty - no
repository source is required.

## Run

```bash
# Create a writable daemon home for the non-root container user
sudo mkdir -p /var/lib/spl-daemon
sudo chown 10001:10001 /var/lib/spl-daemon

docker run -d --name spl-daemon \
  --restart unless-stopped \
  --cap-drop ALL --security-opt no-new-privileges:true \
  -p 127.0.0.1:8765:8765 \
  -v /var/lib/spl-daemon:/var/lib/spl-daemon \
  yastrebovks/spl-daemon:0.4.6
```

## Per-node Docker nodes (0.4 series)

`splime` pipelines can tag individual nodes with the `docker` runtime. That
feature needs the daemon to drive the host Docker engine, so it is **not
available from this containerized daemon**: nested Docker is rejected by
design with a clear error. Run the daemon directly on the host (`pip install
splime && spl-daemon serve`) to use per-node Docker nodes or the object-level
Docker runtime; the container image remains a good fit for trusted venv-based
workloads. Those workers share the container daemon user's identity and daemon
state, so their virtual environments are dependency boundaries, not OS
sandboxes.

## Offline cache and pull mirroring (0.4 series)

## Owner-scoped libraries and @handles

The enrolled daemon addresses libraries as `(owner, slug)`, so a teammate's shared
`default` never blends into yours. Users have unique `@handle`s that you can pass in
any owner slot; the daemon resolves them on the server and stores canonical ids.

```bash
docker exec -it spl-daemon python -c "
from spl import SPLClient; c = SPLClient()
print(c.whoami())                                  # identity and channel state
print(c.libraries())                               # rows carry owner + owned
print(c.library.get('default', owner='@alice'))    # a shared library, explicitly
"
```

The containerized daemon is a full offline cache: it keeps the enrolled owner
identity across container restarts and offline periods, local publishes and
bare-name lookups never wait on the central server, and connected daemons can
mirror visible server objects into the local cache with `spl-daemon pull
<name>` or `spl-daemon pull --all --dry-run` before going offline. Inspect
identity and stored-connection hygiene with `spl-daemon doctor`,
`connections-list`, and `connections-prune --dry-run` (all work inside the
container via `docker exec`).

Or with Compose:

```bash
docker compose up -d
docker compose logs -f
```

### Call the daemon from the host

The HTTP API requires the bearer token the daemon writes to
`daemon-endpoint.json` in its home:

```bash
export SPL_DAEMON_API_TOKEN="$(
  docker exec -t spl-daemon python -c \
    'import json, os; home=os.environ.get("SPL_DAEMON_HOME") or "/var/lib/spl-daemon"; print(json.load(open(os.path.join(home, "daemon-endpoint.json")))["api_token"])'
)"
```

```python
import os
from spl import SPLClient

client = SPLClient(
    base_url="http://127.0.0.1:8765",
    api_token=os.environ["SPL_DAEMON_API_TOKEN"],
)
print(client.health())
```

## Build and publish

```bash
# one-time
docker login                                   # to your Docker Hub account (yastrebovks)
docker buildx create --use --name splime-builder

# build + push multi-arch (version and latest)
./publish.sh 0.4.6
```

To build a single-arch image locally for testing:

```bash
docker build -t yastrebovks/spl-daemon:0.4.6 .
docker run --rm -p 127.0.0.1:8765:8765 \
  -v /var/lib/spl-daemon:/var/lib/spl-daemon yastrebovks/spl-daemon:0.4.6
```

## Security

- The HTTP API requires the bearer token in `daemon-endpoint.json`. Publish the
  port on `127.0.0.1` only - never `0.0.0.0` or a public interface.
- The container runs non-root as `10001:10001`; keep the daemon home writable
  by that identity.
- The host Docker socket is **not** mounted. Mounting `/var/run/docker.sock`
  grants root-equivalent host control and is only needed for docker-runtime
  (DooD) nodes - opt in explicitly and only on trusted machines. See
  [`deploy/daemon/README.md`](../daemon/README.md) for the DooD details.
- `SPL_DAEMON_SECRET_BACKEND=file` keeps secrets in the daemon home instead of
  an OS keyring (there is no desktop keyring in a container).
