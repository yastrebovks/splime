"""Minimal local daemon runtime for SPL objects.

This package is intentionally kept outside ``spl.core``.  The current core
already knows how to describe functions and pipelines as SPL/YAML; the daemon
adds only the missing local runtime pieces:

* a SQLite registry for environments, serialized objects, versions, and runs;
* a local HTTP API available from other Python environments;
* a worker subprocess that executes the object with its registered Python.

The public entry point is the module CLI:

    python -m spl.daemon serve

The package does not import ``spl.core`` here on purpose.  The daemon process
can start and manage its registry even if a particular target environment is
missing the dependencies required by an object; those errors belong to the
worker process and are reported per run.
"""

from spl.daemon.client import Client

__all__ = ["Client"]
