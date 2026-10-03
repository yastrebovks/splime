# splime 0.4.11 — release candidate

This is a prepared candidate, not a publication announcement. Registry artifacts,
the final signed source tag and deployment evidence must be completed before release.

## Native values in public embedded calls

Authors can publish a new public Object version with
`execution_profile="current-process-v1"`. On CPython 3.13, its signed compiled
code runs in the caller's interpreter and can accept and return native Python
objects, including pandas DataFrames and NumPy arrays.

For a published Function that accepts `start_df` and returns a DataFrame:

```python
from spl import SPLClient

# public_ref is the exact URI of a version published with current-process-v1.
run = SPLClient.embedded().call(public_ref, kwargs={"start_df": df}, trust=True)
result_df = run.value
result_df.head()
```

Native Pipeline results retain their alias/output-port structure. Inputs are
passed by reference; published code can modify them. Copy an input explicitly
when its original contents must be preserved.

The profile checks captured dependency metadata before execution. Missing
packages fail with an actionable diagnostic. Different installed versions
produce warnings and are used as installed; semantic compatibility still
depends on the published code. Splime does not install or change these packages.
The initial profile supports CPython 3.13 and native execution, without hard
timeouts or Docker, subprocess and remote node boundaries.

Existing immutable public releases retain their original execution profile.
Selecting `trust=True` approves execution; it does not change a release's
profile. Authors must publish a new version to adopt the new contract.

## Corrections and release controls

- Native result normalization preserves identity, shared references and
  concrete container cycles. Cyclic explicit result-wrapper chains now produce
  a bounded diagnostic instead of a recursion error.
- Release generation updates Console module cache identities, Docker defaults,
  README install commands and the server's exact framework dependency together,
  preserving executable file permissions.
- The server requires explicit selection of a local framework source or wheel
  during coordinated development. Its normal dependency is `splime==0.4.11`.
- The server typing comparison supports an explicitly validated candidate
  interpreter while preserving the frozen historical baseline.

## Validation

Local verification on macOS arm64 / CPython 3.13.14 includes 2,541 SDK/daemon
tests without skips, 1,148 server tests, 572 Console tests, live Docker and
localhost integration coverage, compatibility with a published 0.4.5 daemon,
strict framework typing, the server diagnostic-delta gate and a warning-free
documentation build. Additional tests cover the updated typing verifier.

Wheel and source-distribution installations both execute a signed DataFrame
publication successfully. Native execution was checked with NumPy 2.1.3 /
pandas 2.2.3 and NumPy 2.5.2 / pandas 3.0.3. Repeated wheel and sdist builds have
identical SHA-256 values. These are local results; the remote OS matrix remains
a pre-publication gate.

The final index-based server lock must be generated after `splime==0.4.11`
exists on the package index. No package or service was published or deployed
by this preparation.
