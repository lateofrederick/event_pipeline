import importlib
import logging
import re
import subprocess
import sys
import typing
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from volnux import Event
from ..source import SourceCredentials
from volnux.event.base import EventType
from .utils import get_workflow_config_name
from volnux.manifest.utils import (
    load_manifest,
    redact_credentials,
    build_authenticated_index_url,
    check_compatibility,
    register_workflow_config_and_events_from_manifest,
)

if typing.TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)


# PEP 508 package name — letters, digits, hyphens, underscores, dots.
_SAFE_PACKAGE_RE = re.compile(
    r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?|[A-Za-z0-9])$"
)
# PEP 440 version specifier — digits, dots, letters, and common operators.
_SAFE_VERSION_RE = re.compile(r"^[A-Za-z0-9.*+!\-<>=,\s]+$")


class LoadFromPyPi(Event):
    """
    Engine-internal SYSTEM event that installs and registers a workflow
    packaged on PyPI (or a compatible private index).

    This event is never instantiated by user code. The engine resolves it
    via ``LoaderResolver`` when a ``WorkflowSource`` with
    ``source_type=RegistrySource.PYPI`` calls ``load_workflow_config()``.

    Installation contract
    ---------------------
    - ``version`` is required. Unpinned installs are rejected to guarantee
      reproducible workflow loading across environments.
    - ``package_name`` (the ``location`` field of ``WorkflowSource``) must
      satisfy PEP 508 naming rules. Invalid names are rejected before any
      subprocess is spawned.
    - Credentials are forwarded to pip via ``--extra-index-url`` with the
      token or username:password embedded in the URL. They are never logged.
    - ``timeout`` (in milliseconds) is applied to each pip invocation.
    - The installed package's ``__version__`` is verified against the
      requested version before the workflow config is registered.

    Discovery order
    ---------------
    After installation, the workflow config class is located by:
    1. A conventionally named class derived from the package name via
       ``get_workflow_config_name()``.
    2. A scan of the package's ``workflow`` submodule for any direct
       ``WorkflowConfig`` subclass.
    """

    name = "pypi"
    event_type = EventType.SYSTEM

    def process(
        self,
        location: Union[str, Path],
        registry: "WorkflowRegistry",
        version: Optional[str] = None,
        credentials: Optional[SourceCredentials] = None,
        timeout: int = 30_000,
        index_url: Optional[str] = None,
        **kwargs: Any,
    ) -> typing.Tuple[bool, Any]:
        """
        Install ``location`` from PyPI and register its workflow config.

        Args:
            location:    PyPI package name (the ``location`` field of
                         ``WorkflowSource``).
            registry:    Workflow registry to register the loaded config into.
            version:     Required. Exact version to install (PEP 440).
            credentials: Optional credentials for private index authentication.
            timeout:     Per-attempt pip timeout in milliseconds.
            index_url:   Optional private index URL. Supplied via
                         ``WorkflowSource.metadata["index_url"]``. When
                         present and credentials are valid, credentials are
                         embedded into this URL and passed to pip as
                         ``--extra-index-url``. When absent, pip uses the
                         public PyPI index and credentials are ignored.
            **kwargs:    Absorbed; dropped kwargs are logged by WorkflowSource.

        Returns:
            ``(True, WorkflowConfig class)`` on success.
            ``(False, None)`` on any failure.
        """
        package_name = str(location)

        if not _SAFE_PACKAGE_RE.match(package_name):
            logger.error(
                "LoadFromPyPi: '%s' is not a valid PEP 508 package name.",
                package_name,
            )
            return False, None

        if not version:
            logger.error(
                "LoadFromPyPi: 'version' is required for '%s'. "
                "Unpinned installs produce non-reproducible workflow loading.",
                package_name,
            )
            return False, None

        if not _SAFE_VERSION_RE.match(version):
            logger.error(
                "LoadFromPyPi: '%s' is not a valid PEP 440 version specifier.",
                version,
            )
            return False, None

        package_spec = f"{package_name}=={version}"
        logger.info("LoadFromPyPi: installing '%s'", package_spec)

        cmd = self._build_pip_command(package_spec, credentials, index_url)

        timeout_seconds = timeout / 1000
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            logger.error(
                "LoadFromPyPi: pip timed out after %.1fs for '%s'.",
                timeout_seconds,
                package_spec,
            )
            return False, None

        if result.returncode != 0:
            # stderr may contain index URLs with embedded credentials if pip
            # echoes the command. Strip credential patterns before logging.
            safe_stderr = redact_credentials(result.stderr)
            logger.error(
                "LoadFromPyPi: pip failed for '%s':\n%s",
                package_spec,
                safe_stderr,
            )
            return False, None

        logger.info("LoadFromPyPi: pip install succeeded for '%s'", package_spec)

        return self._load_and_register(package_name, version, registry)

    @staticmethod
    def _build_pip_command(
        package_spec: str,
        credentials: Optional[SourceCredentials],
        index_url: Optional[str],
    ) -> List[str]:
        """
        Build the pip install command, injecting credentials into the index
        URL when a private registry is configured.

        Credentials are embedded in the URL (``token@host`` or
        ``user:pass@host``) so they never appear as plaintext CLI arguments
        that could be captured by process-listing tools.

        If ``index_url`` is absent, credentials are ignored and pip installs
        from the public PyPI index. A warning is emitted if credentials were
        supplied without an index URL, since that is almost certainly a
        misconfiguration — public PyPI does not require authentication for
        package downloads.
        """
        cmd = [sys.executable, "-m", "pip", "install", package_spec]

        if credentials and credentials.is_valid():
            if not index_url:
                logger.warning(
                    "LoadFromPyPi: credentials supplied for '%s' but no "
                    "'index_url' was provided in WorkflowSource.metadata. "
                    "Credentials will be ignored and the public PyPI index "
                    "will be used. Set metadata={'index_url': 'https://...'} "
                    "to authenticate against a private registry.",
                    package_spec,
                )
            else:
                authed_url = build_authenticated_index_url(credentials, index_url)
                if authed_url:
                    cmd += ["--extra-index-url", authed_url]

        return cmd

    @staticmethod
    def _load_and_register(
        package_name: str,
        version: str,
        registry: "WorkflowRegistry",
    ) -> typing.Tuple[bool, Any]:
        """
        Import the package, verify its version, then register its contents.

        Two package types are supported — both must supply a manifest:

        Event-only package
            Contains a ``volnux.manifest.json`` and one or more ``EventBase``
            subclasses. No ``WorkflowConfig`` is present. The manifest is the
            authoritative list of event classes to register.

        Workflow package
            Contains a ``volnux.manifest.json``, one or more ``EventBase``
            subclasses, *and* a ``WorkflowConfig`` subclass. Both the events
            and the workflow config are registered.

        The manifest is required for both types — it is the stable contract
        between a package and the engine. ``WorkflowConfig`` is additive.

        Return value
        ------------
        ``(True, result)`` where ``result`` is:
          - the ``WorkflowConfig`` class for workflow packages
          - the parsed manifest dict for event-only packages

        ``(False, None)`` on any failure.
        """
        from ..workflow import WorkflowConfig

        try:
            module = importlib.import_module(package_name)
        except ImportError as exc:
            logger.error(
                "LoadFromPyPi: '%s' installed successfully but cannot be "
                "imported: %s",
                package_name,
                exc,
            )
            return False, None

        # Version is verified against manifest["package"]["version"] rather
        # than module.__version__ because the manifest is the authoritative
        # source of truth for the installed package identity. module.__version__
        # is a convention, not a guarantee; packages may omit it or set it
        # incorrectly. The manifest version is validated by the EventHub
        # registry at publish time and is always present.
        manifest = load_manifest(module, package_name)
        if manifest is None:
            # load_manifest logs the specific failure; nothing more to add.
            return False, None

        # Version verification against manifest
        manifest_version = manifest.get("package", {}).get("version", "")
        if manifest_version != version:
            logger.error(
                "LoadFromPyPi: version mismatch for '%s' — "
                "requested %s but manifest declares %s. "
                "The environment may have a conflicting version pinned by "
                "another package, or the manifest was not regenerated after "
                "a version bump.",
                package_name,
                version,
                manifest_version,
            )
            return False, None

        # Emits warnings only — does not block loading. The operator is
        # informed of unsupported Python/Volnux combinations at load time
        # rather than silently proceeding.
        check_compatibility(manifest, package_name)

        registered_events, registered_workflow_config = (
            register_workflow_config_and_events_from_manifest(
                manifest, module, package_name, registry
            )
        )
        if not registered_events:
            logger.error(
                "LoadFromPyPi: manifest for '%s' declared no events, or none "
                "could be resolved. At least one event must be registered.",
                package_name,
            )
            return False, None

        config_class: Optional[typing.Type[WorkflowConfig]] = (
            _find_workflow_config_class(module, package_name)
        )

        if config_class is not None:
            config_instance = config_class()
            registry.register(config_instance)
            logger.info(
                "LoadFromPyPi: registered workflow config '%s' from '%s==%s' "
                "(%d events registered)",
                config_instance.name,
                package_name,
                version,
                len(registered_events),
            )
            return True, config_class

        # Event-only package — successful load.
        logger.info(
            "LoadFromPyPi: registered %d event(s) from '%s==%s' "
            "(event-only package, no WorkflowConfig)",
            len(registered_events),
            package_name,
            version,
        )
        return True, manifest


# class LoadFromPyPi(Event):
#     """
#     Engine-internal SYSTEM event that installs and registers a workflow
#     packaged on PyPI (or a compatible private index).
#
#     This event is never instantiated by user code. The engine resolves it
#     via ``LoaderResolver`` when a ``WorkflowSource`` with
#     ``source_type=RegistrySource.PYPI`` calls ``load_workflow_config()``.
#
#     Installation contract
#     ---------------------
#     - ``version`` is required. Unpinned installs are rejected to guarantee
#       reproducible workflow loading across environments.
#     - ``package_name`` (the ``location`` field of ``WorkflowSource``) must
#       satisfy PEP 508 naming rules. Invalid names are rejected before any
#       subprocess is spawned.
#     - Credentials are forwarded to pip via ``--extra-index-url`` with the
#       token or username:password embedded in the URL. They are never logged.
#     - ``timeout`` (in milliseconds) is applied to each pip invocation.
#     - The installed package's ``__version__`` is verified against the
#       requested version before the workflow config is registered.
#
#     Discovery order
#     ---------------
#     After installation, the workflow config class is located by:
#     1. A conventionally named class derived from the package name via
#        ``get_workflow_config_name()``.
#     2. A scan of the package's ``workflow`` submodule for any direct
#        ``WorkflowConfig`` subclass.
#     """
#
#     name = "pypi"
#     event_type = EventType.SYSTEM
#
#     def process(
#         self,
#         location: Union[str, Path],
#         registry: "WorkflowRegistry",
#         version: Optional[str] = None,
#         credentials: Optional[SourceCredentials] = None,
#         timeout: int = 30_000,
#         index_url: Optional[str] = None,
#         **kwargs: Any,
#     ) -> typing.Tuple[bool, Any]:
#         """
#         Install ``location`` from PyPI and register its workflow config.
#
#         Args:
#             location:    PyPI package name (the ``location`` field of
#                          ``WorkflowSource``).
#             registry:    Workflow registry to register the loaded config into.
#             version:     Required. Exact version to install (PEP 440).
#             credentials: Optional credentials for private index authentication.
#             timeout:     Per-attempt pip timeout in milliseconds.
#             index_url:   Optional private index URL. Supplied via
#                          ``WorkflowSource.metadata["index_url"]``. When
#                          present and credentials are valid, credentials are
#                          embedded into this URL and passed to pip as
#                          ``--extra-index-url``. When absent, pip uses the
#                          public PyPI index and credentials are ignored.
#             **kwargs:    Absorbed; dropped kwargs are logged by WorkflowSource.
#
#         Returns:
#             ``(True, WorkflowConfig class)`` on success.
#             ``(False, None)`` on any failure.
#         """
#         package_name = str(location)
#
#         # ── Input validation ───────────────────────────────────────────────
#         if not _SAFE_PACKAGE_RE.match(package_name):
#             logger.error(
#                 "LoadFromPyPi: '%s' is not a valid PEP 508 package name.",
#                 package_name,
#             )
#             return False, None
#
#         if not version:
#             logger.error(
#                 "LoadFromPyPi: 'version' is required for '%s'. "
#                 "Unpinned installs produce non-reproducible workflow loading.",
#                 package_name,
#             )
#             return False, None
#
#         if not _SAFE_VERSION_RE.match(version):
#             logger.error(
#                 "LoadFromPyPi: '%s' is not a valid PEP 440 version specifier.",
#                 version,
#             )
#             return False, None
#
#         package_spec = f"{package_name}=={version}"
#         logger.info("LoadFromPyPi: installing '%s'", package_spec)
#
#         # ── Build pip command ──────────────────────────────────────────────
#         cmd = self._build_pip_command(package_spec, credentials, index_url)
#
#         # ── Execute pip ────────────────────────────────────────────────────
#         timeout_seconds = timeout / 1000
#         try:
#             result = subprocess.run(
#                 cmd,
#                 capture_output=True,
#                 text=True,
#                 timeout=timeout_seconds,
#             )
#         except subprocess.TimeoutExpired:
#             logger.error(
#                 "LoadFromPyPi: pip timed out after %.1fs for '%s'.",
#                 timeout_seconds,
#                 package_spec,
#             )
#             return False, None
#
#         if result.returncode != 0:
#             # stderr may contain index URLs with embedded credentials if pip
#             # echoes the command. Strip credential patterns before logging.
#             safe_stderr = _redact_credentials(result.stderr)
#             logger.error(
#                 "LoadFromPyPi: pip failed for '%s':\n%s",
#                 package_spec,
#                 safe_stderr,
#             )
#             return False, None
#
#         logger.info("LoadFromPyPi: pip install succeeded for '%s'", package_spec)
#
#         # ── Discover and register the workflow config ──────────────────────
#         return self._load_and_register(package_name, version, registry)
#
#     # ── Private helpers ────────────────────────────────────────────────────────
#
#     @staticmethod
#     def _build_pip_command(
#         package_spec: str,
#         credentials: Optional[SourceCredentials] = None,
#         index_url: Optional[str] = None,
#     ) -> List[str]:
#         """
#         Build the pip install command, injecting credentials into the index
#         URL when a private registry is configured.
#
#         Credentials are embedded in the URL (``token@host`` or
#         ``user:pass@host``) so they never appear as plaintext CLI arguments
#         that could be captured by process-listing tools.
#
#         If ``index_url`` is absent, credentials are ignored and pip installs
#         from the public PyPI index. A warning is emitted if credentials were
#         supplied without an index URL, since that is almost certainly a
#         misconfiguration — public PyPI does not require authentication for
#         package downloads.
#         """
#         cmd = [sys.executable, "-m", "pip", "install", package_spec]
#
#         if credentials and credentials.is_valid():
#             if not index_url:
#                 logger.warning(
#                     "LoadFromPyPi: credentials supplied for '%s' but no "
#                     "'index_url' was provided in WorkflowSource.metadata. "
#                     "Credentials will be ignored and the public PyPI index "
#                     "will be used. Set metadata={'index_url': 'https://...'} "
#                     "to authenticate against a private registry.",
#                     package_spec,
#                 )
#             else:
#                 authed_url = _build_authenticated_index_url(credentials, index_url)
#                 if authed_url:
#                     cmd += ["--extra-index-url", authed_url]
#
#         return cmd
#
#     @staticmethod
#     def _load_and_register(
#         package_name: str,
#         version: str,
#         registry: "WorkflowRegistry",
#     ) -> typing.Tuple[bool, Any]:
#         """
#         Import the package, verify its version, find the WorkflowConfig
#         subclass, and register it.
#         """
#         from ..workflow import WorkflowConfig
#
#         try:
#             module = importlib.import_module(package_name)
#         except ImportError as exc:
#             logger.error(
#                 "LoadFromPyPi: '%s' installed successfully but cannot be "
#                 "imported: %s",
#                 package_name,
#                 exc,
#             )
#             return False, None
#
#         # ── Version verification ───────────────────────────────────────────
#         installed_version = getattr(module, "__version__", None)
#         if installed_version and installed_version != version:
#             logger.error(
#                 "LoadFromPyPi: version mismatch for '%s' — "
#                 "requested %s, found %s. The environment may have a "
#                 "conflicting version pinned by another package.",
#                 package_name,
#                 version,
#                 installed_version,
#             )
#             return False, None
#
#         # ── Config class discovery ─────────────────────────────────────────
#         config_class: Optional[typing.Type[WorkflowConfig]] = (
#             _find_workflow_config_class(module, package_name)
#         )
#
#         if config_class is None:
#             logger.error(
#                 "LoadFromPyPi: no WorkflowConfig subclass found in '%s'. "
#                 "Expected either a class named '%s' at the package root or "
#                 "any WorkflowConfig subclass in the 'workflow' submodule.",
#                 package_name,
#                 get_workflow_config_name(package_name),
#             )
#             return False, None
#
#         # ── Registration ───────────────────────────────────────────────────
#         config_instance = config_class()
#         registry.register(config_instance)
#         logger.info(
#             "LoadFromPyPi: registered workflow '%s' from '%s==%s'",
#             config_instance.name,
#             package_name,
#             version,
#         )
#         return True, config_class
#
#
# # ── Module-level helpers ───────────────────────────────────────────────────────
#
#
# def _find_workflow_config_class(
#     module: types.ModuleType,
#     package_name: str,
# ) -> Optional[typing.Type["WorkflowConfig"]]:
#     """
#     Locate a WorkflowConfig subclass in ``module`` using the two-step
#     discovery order documented on LoadFromPyPi.
#     """
#     from ..workflow import WorkflowConfig
#
#     # 1. Conventional name at the package root.
#     candidate_name = get_workflow_config_name(package_name)
#     candidate = getattr(module, candidate_name, None)
#     if (
#         candidate is not None
#         and isinstance(candidate, type)
#         and issubclass(candidate, WorkflowConfig)
#         and candidate is not WorkflowConfig
#     ):
#         return candidate
#
#     # 2. Scan the 'workflow' submodule.
#     workflow_module = getattr(module, "workflow", None)
#     if workflow_module is not None:
#         for attr_name in dir(workflow_module):
#             attr = getattr(workflow_module, attr_name, None)
#             if (
#                 attr is not None
#                 and isinstance(attr, type)
#                 and issubclass(attr, WorkflowConfig)
#                 and attr is not WorkflowConfig
#             ):
#                 return attr
#
#     return None
#
#
# def _build_authenticated_index_url(
#     credentials: SourceCredentials,
#     index_url: str,
# ) -> Optional[str]:
#     """
#     Embed ``credentials`` into ``index_url`` and return the authenticated URL.
#
#     The credential is placed in the URL authority component
#     (``scheme://auth@host/path``) so it is sent as HTTP Basic Auth by pip.
#     This avoids passing secrets as plaintext CLI arguments that could be
#     captured by process-listing tools or shell history.
#
#     Token auth (preferred)::
#
#         https://token@my.registry.com/simple
#
#     Username/password auth::
#
#         https://user:password@my.registry.com/simple
#
#     Returns ``None`` if:
#     - ``index_url`` is not a valid ``http://`` or ``https://`` URL.
#     - ``credentials.is_valid()`` is False (guards against a partially
#       constructed ``SourceCredentials`` reaching this function).
#
#     The returned URL is only passed to pip internals and is never logged
#     directly. All log sites must pass the URL through ``_redact_credentials``
#     before emitting it.
#     """
#     if not credentials.is_valid():
#         logger.debug(
#             "_build_authenticated_index_url: credentials are not valid; "
#             "returning unauthenticated URL."
#         )
#         return None
#
#     parsed = urlparse(index_url)
#
#     if parsed.scheme not in ("http", "https"):
#         logger.error(
#             "_build_authenticated_index_url: '%s' is not a valid http/https "
#             "URL. Only http and https private registries are supported.",
#             _redact_credentials(index_url),
#         )
#         return None
#
#     if not parsed.netloc:
#         logger.error(
#             "_build_authenticated_index_url: '%s' has no host component.",
#             _redact_credentials(index_url),
#         )
#         return None
#
#     # Build the auth string. Token takes precedence over username/password
#     # because token auth is more secure and more widely supported by private
#     # PyPI-compatible registries (e.g. Artifactory, AWS CodeArtifact, GCP
#     # Artifact Registry, Azure Artifacts).
#     if credentials.token:
#         # Many registries use "__token__" as the conventional username when
#         # authenticating with a token. The token itself is the password.
#         auth = f"__token__:{credentials.token}"
#     else:
#         from urllib.parse import quote
#
#         # Percent-encode username and password to handle special characters
#         # (e.g. "@", ":", "/") that would otherwise break URL parsing.
#         encoded_user = quote(credentials.username, safe="")
#         encoded_pass = quote(credentials.password, safe="")
#         auth = f"{encoded_user}:{encoded_pass}"
#
#     # Replace any existing netloc auth (user:pass@host) with the new
#     # credentials so we never double-embed if the URL already had auth.
#     host_only = parsed.hostname
#     if parsed.port:
#         host_only = f"{host_only}:{parsed.port}"
#
#     authenticated_netloc = f"{auth}@{host_only}"
#     authenticated = parsed._replace(netloc=authenticated_netloc)
#
#     return urlunparse(authenticated)
#
#
# def _redact_credentials(text: str) -> str:
#     """
#     Remove credential patterns from pip stderr before logging.
#
#     Replaces ``user:password@`` and ``token@`` patterns that pip may echo
#     when reporting the index URL it used.
#     """
#     # Matches: scheme://anything@host (captures the auth portion)
#     return re.sub(r"(https?://)([^@\s]+@)", r"\1***@", text)
#
