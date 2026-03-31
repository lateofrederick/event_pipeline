"""
Retry with exponential backoff + jitter and a circuit breaker for remote submissions.
"""
from __future__ import annotations

import errno
import logging
import re
import threading
import time
import typing

from volnux.constants import ErrorCodes

logger = logging.getLogger(__name__)

_TRANSIENT_ERRNOS = frozenset(
    {
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.ETIMEDOUT,
        errno.EPIPE,
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
        errno.ECONNABORTED,
    }
)

# Application-level codes that must not be retried.
PERMANENT_SUBMISSION_CODES: typing.FrozenSet[str] = frozenset(
    {
        ErrorCodes.INVALID_CHECKSUM,
        "INVALID_HMAC",
        ErrorCodes.EVENT_NOT_WHITELISTED,
        ErrorCodes.EVENT_NOT_REGISTERED,
    }
)

# Application-level codes that are safe to retry (transient backpressure / validation timing).
TRANSIENT_SUBMISSION_CODES: typing.FrozenSet[str] = frozenset(
    {
        ErrorCodes.QUEUE_FULL,
    }
)


class RemoteSubmitRetryPolicy(typing.NamedTuple):
    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float
    jitter_ratio: float

    @classmethod
    def from_config(cls, conf: typing.Any) -> "RemoteSubmitRetryPolicy":
        return cls(
            max_attempts=int(getattr(conf, "REMOTE_RETRY_MAX_ATTEMPTS", 5)),
            base_delay_seconds=float(getattr(conf, "REMOTE_RETRY_BASE_DELAY", 0.1)),
            max_delay_seconds=float(getattr(conf, "REMOTE_RETRY_MAX_DELAY", 30.0)),
            jitter_ratio=float(getattr(conf, "REMOTE_RETRY_JITTER_RATIO", 0.2)),
        )


class RemoteSubmitCircuitBreakerPolicy(typing.NamedTuple):
    failure_threshold: int
    recovery_timeout_seconds: float
    half_open_max_calls: int

    @classmethod
    def from_config(cls, conf: typing.Any) -> "RemoteSubmitCircuitBreakerPolicy":
        return cls(
            failure_threshold=int(getattr(conf, "REMOTE_CIRCUIT_FAILURE_THRESHOLD", 5)),
            recovery_timeout_seconds=float(
                getattr(conf, "REMOTE_CIRCUIT_RECOVERY_TIMEOUT", 30.0)
            ),
            half_open_max_calls=int(
                getattr(conf, "REMOTE_CIRCUIT_HALF_OPEN_MAX_CALLS", 1)
            ),
        )


def backoff_delay_seconds(
    attempt_index: int,
    policy: RemoteSubmitRetryPolicy,
    random_fn: typing.Callable[[], float],
) -> float:
    """attempt_index is 0-based (first retry after attempt 0)."""
    raw = policy.base_delay_seconds * (2**attempt_index)
    capped = min(raw, policy.max_delay_seconds)
    jr = policy.jitter_ratio
    # Spread in [capped * (1 - jr), capped * (1 + jr)]
    u = random_fn()
    return capped * (1.0 - jr + 2.0 * jr * u)


_CODE_PREFIX_RE = re.compile(r"^([A-Z][A-Z0-9_]*):\s")


def extract_error_code(exc: BaseException) -> typing.Optional[str]:
    from volnux.exceptions import RemoteExecutionError

    if isinstance(exc, RemoteExecutionError) and getattr(exc, "code", None):
        return typing.cast(str, exc.code)
    msg = str(exc).strip()
    if msg in PERMANENT_SUBMISSION_CODES or msg in TRANSIENT_SUBMISSION_CODES:
        return msg
    m = _CODE_PREFIX_RE.match(str(exc))
    if m:
        return m.group(1)
    return None


def is_transient_error(exc: BaseException) -> bool:
    """Return True if the failure may succeed on retry."""
    from volnux.exceptions import RemoteExecutionError

    code = extract_error_code(exc)
    if code:
        if code in PERMANENT_SUBMISSION_CODES:
            return False
        if code in TRANSIENT_SUBMISSION_CODES:
            return True

    if isinstance(exc, RemoteExecutionError):
        return False

    try:
        import grpc as _grpc

        if isinstance(exc, _grpc.RpcError):
            transient_statuses = {
                _grpc.StatusCode.UNAVAILABLE,
                _grpc.StatusCode.DEADLINE_EXCEEDED,
                _grpc.StatusCode.ABORTED,
                _grpc.StatusCode.RESOURCE_EXHAUSTED,
            }
            return exc.code() in transient_statuses
    except Exception:
        pass

    if isinstance(exc, (TimeoutError, BrokenPipeError, ConnectionError)):
        return True
    if isinstance(exc, OSError):
        en = getattr(exc, "errno", None)
        return en in _TRANSIENT_ERRNOS
    return False


class CircuitBreaker:
    """
    Closed -> (N consecutive transient failures) -> Open -> (after recovery) -> Half-open -> Closed/Open.
    """

    def __init__(
        self,
        policy: RemoteSubmitCircuitBreakerPolicy,
        *,
        monotonic_fn: typing.Callable[[], float] = time.monotonic,
    ) -> None:
        self._policy = policy
        self._monotonic = monotonic_fn
        self._lock = threading.Lock()
        self._state: typing.Literal["closed", "open", "half_open"] = "closed"
        self._consecutive_failures = 0
        self._opened_at: typing.Optional[float] = None
        self._half_open_in_flight = 0

    @property
    def state(self) -> str:
        return self._state

    def allow_request(self) -> None:
        from volnux.exceptions import CircuitBreakerOpenError

        with self._lock:
            now = self._monotonic()
            if self._state == "open":
                assert self._opened_at is not None
                if now - self._opened_at < self._policy.recovery_timeout_seconds:
                    raise CircuitBreakerOpenError(
                        "Circuit breaker is open; remote manager calls are temporarily blocked",
                        recovery_in_seconds=self._policy.recovery_timeout_seconds
                        - (now - self._opened_at),
                    )
                self._state = "half_open"
                self._half_open_in_flight = 0

            if self._state == "half_open":
                if self._half_open_in_flight >= self._policy.half_open_max_calls:
                    raise CircuitBreakerOpenError(
                        "Circuit breaker half-open probe already in progress",
                        recovery_in_seconds=None,
                    )
                self._half_open_in_flight += 1

    def record_success(self) -> None:
        with self._lock:
            self._consecutive_failures = 0
            self._opened_at = None
            self._state = "closed"
            self._half_open_in_flight = 0

    def record_transient_exhausted(self) -> None:
        with self._lock:
            if self._state == "half_open":
                self._state = "open"
                self._opened_at = self._monotonic()
                self._half_open_in_flight = 0
                return

            self._consecutive_failures += 1
            if self._consecutive_failures >= self._policy.failure_threshold:
                self._state = "open"
                self._opened_at = self._monotonic()


def execute_with_resilience(
    operation: typing.Callable[[], typing.Any],
    *,
    retry_policy: RemoteSubmitRetryPolicy,
    circuit_breaker: CircuitBreaker,
    sleep_fn: typing.Callable[[float], None],
    random_fn: typing.Callable[[], float],
    log_context: typing.Dict[str, typing.Any],
) -> typing.Any:
    """
    Run ``operation`` with retries on transient errors and circuit breaker accounting.
    """
    circuit_breaker.allow_request()

    last_exc: typing.Optional[BaseException] = None
    max_attempts = max(1, retry_policy.max_attempts)

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation()
            circuit_breaker.record_success()
            return result
        except Exception as e:
            last_exc = e
            if not is_transient_error(e):
                # Definitive application response: remote is reachable.
                circuit_breaker.record_success()
                raise

            will_retry = attempt < max_attempts
            payload = {
                **log_context,
                "volnux_event": "remote_submit_retry",
                "attempt": attempt,
                "max_attempts": max_attempts,
                "will_retry": will_retry,
                "error_type": type(e).__name__,
                "error_code": extract_error_code(e),
                "transient": True,
            }
            logger.info("remote_submit_retry", extra=payload)
            if not will_retry:
                circuit_breaker.record_transient_exhausted()
                raise

            delay = backoff_delay_seconds(attempt - 1, retry_policy, random_fn)
            logger.info(
                "remote_submit_backoff",
                extra={
                    **log_context,
                    "volnux_event": "remote_submit_backoff",
                    "sleep_seconds": delay,
                    "next_attempt": attempt + 1,
                },
            )
            sleep_fn(delay)

    assert last_exc is not None
    raise last_exc
