Release and compatibility evidence
==================================

SPLime 0.4.6 separates a version declaration from evidence that exact source
was built, published, and deployed. A matching version string is useful
context; it is not a deployment attestation.

The reviewed inputs are ``release-contract.json``, the version-2
``release-manifest.json``, and ``release/compatibility-matrix.json``. Run the
single identity generator after changing the planned release:

.. code-block:: console

   python -m tools.generate_release_identity --new-declaration
   python -m tools.generate_release_identity --check

The generator updates public version declarations and the server's bundled
declaration. It does not write a component's own final commit or artifact hash
back into that component repository. A newly generated manifest is therefore
in ``declared`` state and is intentionally not publishable.

Tracked contracts and manifests remain declaration-only. For a pinned
component, the exact post-commit object ID lives in an artifact-side manifest,
outside every component repository. For a signed-tag component, the object ID
is resolved from the verified tag and stays external provenance. The server's
bundled identity remains declaration-only; its deployment receipt carries
commit and hash evidence after deployment.

After the tracked declarations are committed, resolve the signed framework tag
and the pinned server and Console revisions into one external source manifest:

.. code-block:: console

   python -m tools.release_chain \
     --workspace-root .. \
     --manifest release-manifest.json \
     --emit-source-evidence ../artifacts/source-release-manifest.json \
     --observed-at 2026-07-30T12:00:00Z
   python -m tools.release_chain \
     --workspace-root .. \
     --manifest ../artifacts/source-release-manifest.json \
     --stage source

Console ``build.json`` is build evidence, not a tracked declaration. Build the
Console from the exact pinned commit into the external staging root. The
builder creates the allowlisted static tree, artifact-side identity, integrity
manifest, and deterministic archive without changing the source checkout:

.. code-block:: console

   python -m tools.build_console_artifact \
     --workspace-root .. \
     --frontend-root ../spl-frontend \
     --contract release-contract.json \
     --output-root ../artifacts \
     --source-commit <full-commit> \
     --built-at 2026-07-30T12:00:00Z \
     --source-date-epoch <framework-commit-epoch>

Materialize the built BOM only after all four exact artifacts exist:

.. code-block:: console

   python -m tools.release_chain \
     --workspace-root .. \
     --manifest ../artifacts/source-release-manifest.json \
     --emit-built-evidence ../artifacts/release-manifest.json \
     --component-artifact framework=artifacts/python/splime-0.4.6-py3-none-any.whl \
     --component-artifact daemon=artifacts/python/splime-0.4.6-py3-none-any.whl \
     --component-artifact server=artifacts/server/spl_server-0.4.6-py3-none-any.whl \
     --component-artifact console=artifacts/splime-console-0.4.6.tar.gz \
     --observed-at 2026-07-30T12:00:00Z

The normal identity-generator ``--check`` requires the tracked manifest to
remain the exact declaration. ``--new-declaration`` is the only command that
resets tracked evidence to null. The release workflow is the authoritative
orchestration for source resolution, reproducible builds, and checksum
inventory.

After PyPI and the multi-architecture Docker image are published, bind the
observed PyPI filenames/URLs/hashes, reviewed GitHub asset bytes, the observed
public cookbook hash, and Docker's immutable manifest/platform digests into
the external published manifest. The final PyPI workflow job uploads
``pypi-publication-evidence.json`` as the exact operator handoff:

.. code-block:: console

   python -m tools.release_chain \
     --workspace-root .. \
     --manifest ../artifacts/release-manifest.json \
     --emit-published-evidence ../artifacts/published/release-manifest.json \
     --pypi-artifact splime-0.4.6-py3-none-any.whl=https://files.pythonhosted.org/packages/<reviewed-path>/splime-0.4.6-py3-none-any.whl=<sha256> \
     --pypi-artifact splime-0.4.6.tar.gz=https://files.pythonhosted.org/packages/<reviewed-path>/splime-0.4.6.tar.gz=<sha256> \
     --github-asset source-release-manifest.json=artifacts/source-release-manifest.json \
     --github-asset release-artifact-bom.sha256=artifacts/release-artifact-bom.sha256 \
     --github-asset splime-0.4.6-py3-none-any.whl=artifacts/python/splime-0.4.6-py3-none-any.whl \
     --github-asset splime-0.4.6.tar.gz=artifacts/python/splime-0.4.6.tar.gz \
     --github-asset spl_server-0.4.6-py3-none-any.whl=artifacts/server/spl_server-0.4.6-py3-none-any.whl \
     --github-asset splime-console-0.4.6.tar.gz=artifacts/splime-console-0.4.6.tar.gz \
     --github-asset static-integrity.json=artifacts/console/static-integrity.json \
     --public-artifact https://splime.io/downloads/splime-cookbook.ipynb=<sha256> \
     --docker-manifest-digest sha256:<multi-arch-digest> \
     --docker-platform-digest linux/amd64=sha256:<amd64-digest> \
     --docker-platform-digest linux/arm64=sha256:<arm64-digest> \
     --observed-at 2026-07-30T12:30:00Z

Every input set must exactly equal the declaration; duplicate, missing,
unexpected, non-artifact-side, malformed, or byte-mismatched inputs fail
closed. PyPI URLs must be credential-free HTTPS URLs ending in the exact
declared filenames, and their hashes must match the validated built bytes.
During materialization the external ``release-artifact-bom.sha256`` is rebuilt
atomically from the exact non-self-referential artifact set before its own
GitHub asset hash is bound. Neither the built nor final
``release-manifest.json`` is listed in that inventory or as its own GitHub
asset hash because doing so would create a content cycle. The final manifest's
exact SHA-256 is an independent release-review/workflow input.

Evidence gates
--------------

The cross-repository verifier has five cumulative stages:

.. code-block:: console

   python -m tools.release_chain --stage contract
   python -m tools.release_chain --stage source --manifest ../artifacts/source-release-manifest.json
   python -m tools.release_chain --stage built --manifest ../artifacts/release-manifest.json
   python -m tools.release_chain --stage published \
     --manifest ../artifacts/published/release-manifest.json
   python -m tools.release_chain --stage deployed \
     --manifest ../artifacts/published/release-manifest.json \
     --receipt /secure/read-only/server-receipt.json

``source`` requires every component's full commit to match either its verified
signed tag or the external post-commit pin, and requires every repository to be
clean. Source, build, and deployment stages reject a manifest inside a source
repository. ``built`` additionally verifies exact staged component bytes and
every Console asset, including the artifact-side ``build.json``.
``published`` additionally rehashes every declared GitHub release asset and
requires exact observed PyPI filenames/URLs/hashes plus the Docker
multi-architecture and ``linux/amd64`` / ``linux/arm64`` digests.
``deployed`` binds the staged server to the release manifest digest and schema
target. Operational ``/health`` and ``/ready`` checks remain separate.

After publication, the public-asset verifier also requires the actual server
receipt and readiness endpoints. Their URLs are operator inputs rather than
being guessed from source metadata:

.. code-block:: console

   python -m tools.verify_published_release \
     --server-version-url https://server.example/version \
     --server-ready-url https://server.example/ready

The ``/version`` receipt must match the manifest's server source, artifact,
manifest digest, and schema target. The independent ``/ready`` response must
then report operational readiness. Both responses must be explicitly
revalidated so a cached receipt cannot be promoted as current evidence.

The host-controlled server receipt is read-only to the service and contains
only release ID, version, source ref and commit, artifact and manifest SHA-256,
schema target, deployment time, and environment class. Hostnames, paths,
environment variables, credentials, and account data are forbidden.

Compatibility
-------------

Console and server are lockstep during external alpha. Daemon/server
compatibility is capability-negotiated. ``N-1`` is promised only for an
individual matrix row with a complete test gate; it is never inferred from the
package number. Missing build, protocol, or receipt evidence remains
``unknown`` and cannot be rendered as verified.

``spl.worker_build.v1`` is a separately negotiated, allowlisted observation of
the installed Worker package. The current-train matrix verifies its
daemon-to-server-to-Console projection. Missing or invalid evidence remains
``unknown``; an observed package version does not prove execution
compatibility, capacity, readiness, artifact provenance, or source identity.

The standalone historical ``spl-daemon`` and ``spl-core`` repositories are not
release components for this architecture. Framework and daemon point to the
same monolithic ``splime`` artifact until that architecture is explicitly
replaced.
