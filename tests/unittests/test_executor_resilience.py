import errno
import unittest

from volnux.exceptions import CircuitBreakerOpenError, RemoteExecutionError
from volnux.executors.resilience import (
    CircuitBreaker,
    RemoteSubmitCircuitBreakerPolicy,
    RemoteSubmitRetryPolicy,
    backoff_delay_seconds,
    execute_with_resilience,
    extract_error_code,
    is_transient_error,
)


class TestExecutorResilience(unittest.TestCase):
    def test_extract_error_code_remote_execution_error_with_code(self):
        exc = RemoteExecutionError("x", code="QUEUE_FULL")
        self.assertEqual(extract_error_code(exc), "QUEUE_FULL")

    def test_is_transient_queue_full(self):
        exc = RemoteExecutionError("QUEUE_FULL: busy", code="QUEUE_FULL")
        self.assertTrue(is_transient_error(exc))

    def test_is_permanent_invalid_checksum(self):
        exc = RemoteExecutionError("bad", code="INVALID_CHECKSUM")
        self.assertFalse(is_transient_error(exc))

    def test_is_transient_os_error_conn_refused(self):
        err = OSError(errno.ECONNREFUSED, "refused")
        self.assertTrue(is_transient_error(err))

    def test_backoff_capped_and_jitter(self):
        policy = RemoteSubmitRetryPolicy(
            max_attempts=5,
            base_delay_seconds=1.0,
            max_delay_seconds=5.0,
            jitter_ratio=0.0,
        )
        # attempt_index=3 -> raw 8, capped to 5
        self.assertEqual(backoff_delay_seconds(3, policy, lambda: 0.5), 5.0)

    def test_execute_retries_then_succeeds(self):
        policy = RemoteSubmitRetryPolicy(4, 0.01, 1.0, 0.0)
        cb_policy = RemoteSubmitCircuitBreakerPolicy(10, 60.0, 1)
        breaker = CircuitBreaker(cb_policy, monotonic_fn=lambda: 0.0)
        sleeps: list[float] = []
        n = {"i": 0}

        def op():
            n["i"] += 1
            if n["i"] < 3:
                raise OSError(errno.ECONNREFUSED, "refused")
            return "ok"

        out = execute_with_resilience(
            op,
            retry_policy=policy,
            circuit_breaker=breaker,
            sleep_fn=lambda s: sleeps.append(s),
            random_fn=lambda: 0.5,
            log_context={"correlation_id": "c1"},
        )
        self.assertEqual(out, "ok")
        self.assertEqual(n["i"], 3)
        self.assertEqual(len(sleeps), 2)

    def test_execute_no_retry_on_permanent(self):
        policy = RemoteSubmitRetryPolicy(5, 0.01, 1.0, 0.0)
        cb_policy = RemoteSubmitCircuitBreakerPolicy(10, 60.0, 1)
        breaker = CircuitBreaker(cb_policy, monotonic_fn=lambda: 0.0)
        calls = {"n": 0}

        def op():
            calls["n"] += 1
            raise RemoteExecutionError("nope", code="INVALID_CHECKSUM")

        with self.assertRaises(RemoteExecutionError):
            execute_with_resilience(
                op,
                retry_policy=policy,
                circuit_breaker=breaker,
                sleep_fn=lambda s: None,
                random_fn=lambda: 0.5,
                log_context={},
            )
        self.assertEqual(calls["n"], 1)

    def test_circuit_opens_after_threshold(self):
        policy = RemoteSubmitRetryPolicy(2, 0.01, 1.0, 0.0)
        cb_policy = RemoteSubmitCircuitBreakerPolicy(2, 60.0, 1)
        monotonic = {"t": 0.0}

        def mono():
            return monotonic["t"]

        breaker = CircuitBreaker(cb_policy, monotonic_fn=mono)

        def failing_op():
            raise OSError(errno.ECONNREFUSED, "refused")

        with self.assertRaises(OSError):
            execute_with_resilience(
                failing_op,
                retry_policy=policy,
                circuit_breaker=breaker,
                sleep_fn=lambda s: None,
                random_fn=lambda: 0.5,
                log_context={},
            )
        with self.assertRaises(OSError):
            execute_with_resilience(
                failing_op,
                retry_policy=policy,
                circuit_breaker=breaker,
                sleep_fn=lambda s: None,
                random_fn=lambda: 0.5,
                log_context={},
            )
        self.assertEqual(breaker.state, "open")

        monotonic["t"] = 10.0
        with self.assertRaises(CircuitBreakerOpenError):
            execute_with_resilience(
                failing_op,
                retry_policy=policy,
                circuit_breaker=breaker,
                sleep_fn=lambda s: None,
                random_fn=lambda: 0.5,
                log_context={},
            )
