"""Secret storage for local daemon credentials.

The daemon keeps metadata in SQLite, but central-server tokens should live in
an OS credential store when one is available.  A 0600 file backend exists for
tests and headless containers where an OS keychain is unavailable.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Protocol


SECRET_BACKEND_ENV = "SPL_DAEMON_SECRET_BACKEND"
KEYRING_SERVICE = "io.splime.daemon"
FILE_BACKEND_NAME = "file"
KEYRING_BACKEND_NAME = "keyring"


class SecretStoreError(RuntimeError):
    """Raised when a daemon secret cannot be read or written."""


class SecretBackend(Protocol):
    name: str

    def get(self, key: str) -> str | None:
        ...

    def set(self, key: str, value: str) -> None:
        ...

    def delete(self, key: str) -> None:
        ...


class KeyringSecretBackend:
    name = KEYRING_BACKEND_NAME

    def __init__(self, home: Path):
        try:
            import keyring
            import keyring.errors
        except ModuleNotFoundError as exc:
            raise SecretStoreError("keyring package is not installed") from exc
        self._keyring = keyring
        self._errors = keyring.errors
        self._prefix = home.absolute().as_posix()

    def _account(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def get(self, key: str) -> str | None:
        try:
            return self._keyring.get_password(KEYRING_SERVICE, self._account(key))
        except self._errors.KeyringError as exc:
            raise SecretStoreError(str(exc)) from exc

    def set(self, key: str, value: str) -> None:
        try:
            self._keyring.set_password(KEYRING_SERVICE, self._account(key), value)
        except self._errors.KeyringError as exc:
            raise SecretStoreError(str(exc)) from exc

    def delete(self, key: str) -> None:
        try:
            self._keyring.delete_password(KEYRING_SERVICE, self._account(key))
        except self._errors.PasswordDeleteError:
            return
        except self._errors.KeyringError as exc:
            raise SecretStoreError(str(exc)) from exc


class FileSecretBackend:
    name = FILE_BACKEND_NAME

    def __init__(self, home: Path):
        self.path = home / "daemon-secrets.json"

    def _read(self) -> dict[str, str]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        if not isinstance(raw, dict):
            raise SecretStoreError(f"secret file is not a JSON object: {self.path}")
        return {str(key): str(value) for key, value in raw.items()}

    def _write(self, values: dict[str, str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        tmp_path.write_text(
            json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        try:
            tmp_path.chmod(0o600)
        except OSError:
            pass
        tmp_path.replace(self.path)
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def get(self, key: str) -> str | None:
        return self._read().get(key)

    def set(self, key: str, value: str) -> None:
        values = self._read()
        values[key] = value
        self._write(values)

    def delete(self, key: str) -> None:
        values = self._read()
        if key not in values:
            return
        del values[key]
        self._write(values)


class SecretStore:
    """Read and write daemon secrets through keychain-backed refs."""

    def __init__(self, home: Path):
        self.home = home
        self._fallback = FileSecretBackend(home)
        backend = os.environ.get(SECRET_BACKEND_ENV, "auto").strip().lower()
        if backend == FILE_BACKEND_NAME:
            self._write_backends: list[SecretBackend] = [self._fallback]
        elif backend in {"keyring", "os", "keychain"}:
            self._write_backends = [KeyringSecretBackend(home)]
        elif backend in {"", "auto"}:
            self._write_backends = self._auto_backends(home)
        else:
            raise SecretStoreError(f"unknown daemon secret backend: {backend}")
        self._read_backends: dict[str, SecretBackend] = {
            FILE_BACKEND_NAME: self._fallback,
        }
        for candidate in self._write_backends:
            self._read_backends[candidate.name] = candidate

    def _auto_backends(self, home: Path) -> list[SecretBackend]:
        try:
            return [KeyringSecretBackend(home), self._fallback]
        except SecretStoreError:
            return [self._fallback]

    def put(self, key: str, value: str) -> str:
        errors: list[str] = []
        for backend in self._write_backends:
            try:
                backend.set(key, value)
                return f"{backend.name}:{key}"
            except SecretStoreError as exc:
                errors.append(f"{backend.name}: {exc}")
        raise SecretStoreError("; ".join(errors) or "no secret backend available")

    def get(self, ref: str) -> str:
        backend_name, key = self._split_ref(ref)
        backend = self._read_backends.get(backend_name)
        if backend is None:
            raise SecretStoreError(f"unknown secret backend in ref: {backend_name}")
        value = backend.get(key)
        if value is None:
            raise SecretStoreError(f"daemon secret is not found: {ref}")
        return value

    def delete(self, ref: str | None) -> None:
        if not ref:
            return
        backend_name, key = self._split_ref(ref)
        backend = self._read_backends.get(backend_name)
        if backend is None:
            return
        backend.delete(key)

    def _split_ref(self, ref: str) -> tuple[str, str]:
        try:
            backend_name, key = ref.split(":", 1)
        except ValueError as exc:
            raise SecretStoreError(f"invalid daemon secret ref: {ref}") from exc
        if not backend_name or not key:
            raise SecretStoreError(f"invalid daemon secret ref: {ref}")
        return backend_name, key
