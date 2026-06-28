"""Command line entry point for ``python -m spl.daemon``.

Keeping this file tiny makes the module invocation predictable: all argument
parsing lives in ``cli.py`` and this file simply delegates to it.
"""

from spl.daemon.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
