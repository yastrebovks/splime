# SPLime 0.4.10

SPLime 0.4.10 adds complete adapter-backed artifact transport for isolated
per-node runtimes. Non-JSON inputs and outputs now cross native,
`venv-subprocess`, and Docker boundaries with exact save/load dependency
closure, integrity validation, deterministic multi-format fan-out, retention,
and frozen resume support.

The transport remains backward compatible with the legacy inline JSON path and
manifest schema. Malformed descriptors, unsafe paths, provenance mismatches,
cache substitution, and coordinated manifest tampering fail before decoding or
user code. Anonymous Public Object receipts also receive a bounded shutdown
flush so short-lived embedded clients can report ordered terminal state.

```bash
python3.13 -m pip install --upgrade "splime==0.4.10"
```
