"""
Unit tests for the Pointy semantic analyser (Sema).

Each test class covers one category of diagnostic:

  TestSemaClean                — clean programs produce zero diagnostics
  TestSemaDuplicateAttributes  — duplicate attr names → error
  TestSemaDuplicateDescriptors — duplicate descriptors in conditional → error
  TestSemaUnknownDirective     — unknown directive name → error
  TestSemaUnknownOptionKey     — unknown option key → warning
  TestSemaTypeMismatch         — known option key with wrong literal type → warning
  TestSemaUnusedVariable       — @var declared but $var never used → warning
  TestSemaNamingConvention     — non-PascalCase task / template name → warning
"""
import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.sema import Sema
from volnux.parser.sema_errors import SemanticLevel


def _analyse(source: str):
    """Parse *source* and run the semantic pass, returning a SemanticResult."""
    program = pointy_parser(source)
    return Sema().analyse(program)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _errors(result):
    return result.errors


def _warnings(result):
    return result.warnings


def _has_error_containing(result, text: str) -> bool:
    return any(text in d.message for d in result.errors)


def _has_warning_containing(result, text: str) -> bool:
    return any(text in d.message for d in result.warnings)


# ===========================================================================
# Clean programs
# ===========================================================================

class TestSemaClean(unittest.TestCase):
    """A correctly written Pointy program should produce no diagnostics."""

    def _assert_clean(self, source: str) -> None:
        result = _analyse(source)
        self.assertFalse(
            result.errors,
            f"Expected no errors but got: {result.errors}",
        )
        self.assertFalse(
            result.warnings,
            f"Expected no warnings but got: {result.warnings}",
        )

    def test_single_task(self):
        self._assert_clean("FetchData")

    def test_namespaced_task(self):
        self._assert_clean("pypi::Run")

    def test_task_with_known_option(self):
        self._assert_clean("Worker[retry_attempts = 3]")

    def test_task_with_multiple_known_options(self):
        self._assert_clean(
            'Worker[retry_attempts = 3, stop_condition = "on_error"]'
        )

    def test_task_with_bool_option(self):
        self._assert_clean("Worker[bypass_event_checks = true]")

    def test_sequential_chain(self):
        self._assert_clean("FetchData -> ProcessResult -> StoreOutput")

    def test_parallel_chain(self):
        self._assert_clean("FetchData || ProcessResult")

    def test_retry_node(self):
        self._assert_clean("FetchData * 3")

    def test_meta_task_simple(self):
        self._assert_clean("MAP<FetchData>")

    def test_meta_task_namespaced(self):
        self._assert_clean("FILTER<pypi::RunJob>")

    def test_meta_task_with_known_option(self):
        self._assert_clean("MAP<FetchData>[retry_attempts = 2]")

    def test_grouping_no_options(self):
        self._assert_clean("{FetchData -> ProcessResult}")

    def test_grouping_with_known_option(self):
        self._assert_clean("{FetchData -> ProcessResult}[retry_attempts = 2]")

    def test_conditional_unique_descriptors(self):
        self._assert_clean("TaskA(0 -> TaskB, 1 -> TaskC)")

    def test_variable_declared_and_used(self):
        self._assert_clean('@v = 42 Worker[retry_attempts = $v]')

    def test_known_directive_mode(self):
        self._assert_clean('@mode: "CFG" FetchData')

    def test_env_var_reference(self):
        # $env.VAR references are not user-declared variables, should be clean
        self._assert_clean("Worker[executor = $env.EXECUTOR]")

    def test_ternary_in_attribute(self):
        self._assert_clean('Worker[retry_attempts = (1 ?? 2) ? 3 : 4]')

    def test_null_coalesce_in_attribute(self):
        self._assert_clean('Worker[retry_attempts = 1 ?? 2]')

    def test_complex_chain_clean(self):
        self._assert_clean(
            'FetchData[retry_attempts = 3] -> MAP<ProcessPayment>[retry_attempts = 2]'
            ' -> pypi::Run[stop_condition = "on_error"]'
        )


# ===========================================================================
# Duplicate attribute names  →  ERROR
# ===========================================================================

class TestSemaDuplicateAttributes(unittest.TestCase):
    """Duplicate attribute names inside a [] list are a semantic error."""

    def test_task_duplicate_attribute(self):
        result = _analyse('Worker[opt = 1, opt = 2]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'opt'"))

    def test_task_duplicate_attribute_three_attrs(self):
        result = _analyse('Worker[a = 1, b = 2, a = 3]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'a'"))
        # 'b' is unique, should not be flagged
        self.assertFalse(_has_error_containing(result, "Duplicate attribute 'b'"))

    def test_task_two_duplicate_pairs(self):
        result = _analyse('Worker[a = 1, b = 2, a = 3, b = 4]')
        errors = [d for d in result.errors if "Duplicate attribute" in d.message]
        self.assertEqual(len(errors), 2)

    def test_namespaced_task_duplicate_attribute(self):
        result = _analyse('pypi::Run[level = 1, level = 2]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'level'"))

    def test_meta_task_duplicate_attribute(self):
        result = _analyse('MAP<FetchData>[concurrency = 4, concurrency = 8]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'concurrency'"))

    def test_meta_task_namespaced_duplicate_attribute(self):
        result = _analyse('FILTER<pypi::RunJob>[opt = 1, opt = 2]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'opt'"))

    def test_grouping_duplicate_attribute(self):
        result = _analyse('{FetchData}[opt = 1, opt = 2]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'opt'"))

    def test_unique_attributes_no_error(self):
        result = _analyse('Worker[a = 1, b = 2, c = 3]')
        self.assertFalse(result.has_errors)

    def test_duplicate_in_sequential_chain_right(self):
        # Duplicate is on the right-hand task
        result = _analyse('FetchData -> Worker[opt = 1, opt = 2]')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'opt'"))

    def test_duplicate_in_sequential_chain_left(self):
        result = _analyse('Worker[opt = 1, opt = 2] -> FetchData')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'opt'"))

    def test_duplicate_inside_retry_job(self):
        result = _analyse('Worker[opt = 1, opt = 2] * 3')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate attribute 'opt'"))


# ===========================================================================
# Duplicate descriptors  →  ERROR
# ===========================================================================

class TestSemaDuplicateDescriptors(unittest.TestCase):
    """Duplicate descriptor values in a conditional are a semantic error."""

    def test_duplicate_descriptor_zero(self):
        result = _analyse('TaskA(0 -> TaskB, 0 -> TaskC)')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate descriptor '0'"))

    def test_duplicate_descriptor_nonzero(self):
        result = _analyse('TaskA(0 -> TaskB, 1 -> TaskC, 1 -> TaskD)')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate descriptor '1'"))

    def test_two_duplicate_descriptor_pairs(self):
        result = _analyse('TaskA(0 -> TaskB, 0 -> TaskC, 1 -> TaskD, 1 -> TaskE)')
        errors = [d for d in result.errors if "Duplicate descriptor" in d.message]
        self.assertEqual(len(errors), 2)

    def test_unique_descriptors_no_error(self):
        result = _analyse('TaskA(0 -> TaskB, 1 -> TaskC, 2 -> TaskD)')
        self.assertFalse(result.has_errors)

    def test_duplicate_descriptor_in_nested_conditional(self):
        # Duplicate in the outer level
        result = _analyse('TaskA(0 -> TaskB(0 -> TaskC, 1 -> TaskD), 0 -> TaskE)')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Duplicate descriptor '0'"))

    def test_single_branch_no_error(self):
        result = _analyse('TaskA(0 -> TaskB)')
        self.assertFalse(result.has_errors)


# ===========================================================================
# Unknown directive  →  ERROR
# ===========================================================================

class TestSemaUnknownDirective(unittest.TestCase):
    """Unknown directive names produce a semantic error."""

    def test_unknown_directive(self):
        result = _analyse('@unknown: 1 FetchData')
        self.assertTrue(result.has_errors)
        self.assertTrue(_has_error_containing(result, "Unknown directive '@unknown'"))

    def test_unknown_directive_message_lists_known(self):
        result = _analyse('@bogus: 1 FetchData')
        self.assertTrue(result.has_errors)
        err = result.errors[0]
        self.assertIn("mode", err.message)

    def test_known_directive_mode_no_error(self):
        result = _analyse('@mode: "CFG" FetchData')
        errors = [d for d in result.errors if "Unknown directive" in d.message]
        self.assertEqual(len(errors), 0)

    def test_multiple_unknown_directives(self):
        result = _analyse('@foo: 1 @bar: 2 FetchData')
        unknown_errors = [d for d in result.errors if "Unknown directive" in d.message]
        self.assertEqual(len(unknown_errors), 2)

    def test_one_known_one_unknown(self):
        result = _analyse('@mode: "CFG" @typo: 1 FetchData')
        unknown_errors = [d for d in result.errors if "Unknown directive" in d.message]
        self.assertEqual(len(unknown_errors), 1)
        self.assertIn("typo", unknown_errors[0].message)


# ===========================================================================
# Unknown option key  →  WARNING
# ===========================================================================

class TestSemaUnknownOptionKey(unittest.TestCase):
    """Attribute keys that are not in the Options field set produce warnings."""

    def test_unknown_key_single(self):
        result = _analyse('Worker[mystuff = 1]')
        self.assertTrue(_has_warning_containing(result, "Unknown option 'mystuff'"))

    def test_unknown_key_message_mentions_extras(self):
        result = _analyse('Worker[mystuff = 1]')
        w = result.warnings[0]
        self.assertIn("extras", w.message)

    def test_known_key_no_warning(self):
        result = _analyse('Worker[retry_attempts = 3]')
        self.assertFalse(_has_warning_containing(result, "Unknown option"))

    def test_unknown_key_on_namespaced_task(self):
        result = _analyse('pypi::Run[custom_flag = true]')
        self.assertTrue(_has_warning_containing(result, "Unknown option 'custom_flag'"))

    def test_unknown_key_on_meta_task(self):
        result = _analyse('MAP<FetchData>[workers = 8]')
        self.assertTrue(_has_warning_containing(result, "Unknown option 'workers'"))

    def test_unknown_key_on_grouping(self):
        result = _analyse('{FetchData}[my_opt = 1]')
        self.assertTrue(_has_warning_containing(result, "Unknown option 'my_opt'"))

    def test_multiple_unknown_keys(self):
        result = _analyse('Worker[foo = 1, bar = 2]')
        unknown_warnings = [
            d for d in result.warnings if "Unknown option" in d.message
        ]
        self.assertEqual(len(unknown_warnings), 2)

    def test_mix_known_and_unknown(self):
        result = _analyse('Worker[retry_attempts = 3, custom = 1]')
        unknown_warnings = [
            d for d in result.warnings if "Unknown option" in d.message
        ]
        self.assertEqual(len(unknown_warnings), 1)
        self.assertIn("custom", unknown_warnings[0].message)

    def test_all_known_options_no_warning(self):
        result = _analyse(
            'Worker[retry_attempts = 3, stop_condition = "on_error",'
            ' bypass_event_checks = true]'
        )
        self.assertFalse(_has_warning_containing(result, "Unknown option"))


# ===========================================================================
# Type mismatch on known option  →  WARNING
# ===========================================================================

class TestSemaTypeMismatch(unittest.TestCase):
    """Known option keys with incompatible literal value types produce warnings."""

    def test_retry_attempts_expects_int_got_string(self):
        result = _analyse('Worker[retry_attempts = "hello"]')
        self.assertTrue(_has_warning_containing(result, "retry_attempts"))
        self.assertTrue(_has_warning_containing(result, "int"))

    def test_retry_attempts_int_no_warning(self):
        result = _analyse('Worker[retry_attempts = 5]')
        self.assertFalse(_has_warning_containing(result, "retry_attempts"))

    def test_bypass_event_checks_expects_bool_got_int(self):
        # 1 is an int in Pointy; bypass_event_checks expects bool
        result = _analyse('Worker[bypass_event_checks = 1]')
        self.assertTrue(_has_warning_containing(result, "bypass_event_checks"))

    def test_bypass_event_checks_bool_no_warning(self):
        result = _analyse('Worker[bypass_event_checks = true]')
        self.assertFalse(_has_warning_containing(result, "bypass_event_checks"))

    def test_executor_expects_string_got_int(self):
        result = _analyse('Worker[executor = 42]')
        self.assertTrue(_has_warning_containing(result, "executor"))
        self.assertTrue(_has_warning_containing(result, "str"))

    def test_executor_string_no_warning(self):
        result = _analyse('Worker[executor = "DEFAULT_EXECUTOR"]')
        self.assertFalse(_has_warning_containing(result, "executor"))

    def test_stop_condition_string_no_warning(self):
        result = _analyse('Worker[stop_condition = "on_error"]')
        self.assertFalse(_has_warning_containing(result, "stop_condition"))

    def test_stop_condition_int_warns(self):
        result = _analyse('Worker[stop_condition = 1]')
        self.assertTrue(_has_warning_containing(result, "stop_condition"))

    def test_result_evaluation_strategy_string_no_warning(self):
        result = _analyse('Worker[result_evaluation_strategy = "ALL_MUST_SUCCEED"]')
        self.assertFalse(_has_warning_containing(result, "result_evaluation_strategy"))

    def test_result_evaluation_strategy_int_warns(self):
        result = _analyse('Worker[result_evaluation_strategy = 0]')
        self.assertTrue(_has_warning_containing(result, "result_evaluation_strategy"))

    def test_mismatch_in_sequential_right_task(self):
        result = _analyse('FetchData -> Worker[retry_attempts = "bad"]')
        self.assertTrue(_has_warning_containing(result, "retry_attempts"))

    def test_mismatch_on_namespaced_task(self):
        result = _analyse('pypi::Run[retry_attempts = "nope"]')
        self.assertTrue(_has_warning_containing(result, "retry_attempts"))


# ===========================================================================
# Unused variable  →  WARNING
# ===========================================================================

class TestSemaUnusedVariable(unittest.TestCase):
    """Variables declared with @var but never referenced via $var produce warnings."""

    def test_declared_but_never_used(self):
        result = _analyse('@v = 42 Worker')
        self.assertTrue(_has_warning_containing(result, "Variable 'v'"))
        self.assertTrue(_has_warning_containing(result, "never referenced"))

    def test_declared_and_used_no_warning(self):
        result = _analyse('@v = 42 Worker[retry_attempts = $v]')
        self.assertFalse(_has_warning_containing(result, "never referenced"))

    def test_two_vars_one_used(self):
        result = _analyse('@a = 1 @b = 2 Worker[retry_attempts = $a]')
        unused = [d for d in result.warnings if "never referenced" in d.message]
        self.assertEqual(len(unused), 1)
        self.assertIn("'b'", unused[0].message)

    def test_two_vars_both_used_no_warning(self):
        result = _analyse('@a = 1 @b = 2 Worker[retry_attempts = $a, stop_condition = $b]')
        unused = [d for d in result.warnings if "never referenced" in d.message]
        self.assertEqual(len(unused), 0)

    def test_two_vars_both_unused(self):
        result = _analyse('@a = 1 @b = 2 Worker')
        unused = [d for d in result.warnings if "never referenced" in d.message]
        self.assertEqual(len(unused), 2)
        names = {d.message for d in unused}
        self.assertTrue(any("'a'" in n for n in names))
        self.assertTrue(any("'b'" in n for n in names))

    def test_var_used_in_meta_task_attribute(self):
        result = _analyse('@c = 4 MAP<FetchData>[retry_attempts = $c]')
        unused = [d for d in result.warnings if "never referenced" in d.message]
        self.assertEqual(len(unused), 0)

    def test_var_used_in_grouping_attribute(self):
        result = _analyse('@n = 2 {FetchData}[retry_attempts = $n]')
        unused = [d for d in result.warnings if "never referenced" in d.message]
        self.assertEqual(len(unused), 0)

    def test_env_var_does_not_count_as_declared(self):
        # $env.PATH is an env var reference, not a user variable — should not warn
        result = _analyse('Worker[executor = $env.PATH]')
        self.assertFalse(_has_warning_containing(result, "never referenced"))

    def test_unused_var_in_complex_chain(self):
        result = _analyse('@x = 10 FetchData -> Worker[retry_attempts = 3]')
        self.assertTrue(_has_warning_containing(result, "Variable 'x'"))


# ===========================================================================
# Naming convention (PascalCase)  →  WARNING
# ===========================================================================

class TestSemaNamingConvention(unittest.TestCase):
    """Non-PascalCase task or template names produce a warning (recommendation only)."""

    def test_snake_case_task_warns(self):
        result = _analyse('fetch_data')
        self.assertTrue(_has_warning_containing(result, "fetch_data"))
        self.assertTrue(_has_warning_containing(result, "PascalCase"))

    def test_lowercase_task_warns(self):
        result = _analyse('worker')
        self.assertTrue(_has_warning_containing(result, "worker"))

    def test_camel_case_task_warns(self):
        result = _analyse('fetchData')
        self.assertTrue(_has_warning_containing(result, "fetchData"))

    def test_pascal_case_task_no_warning(self):
        result = _analyse('FetchData')
        self.assertFalse(_has_warning_containing(result, "PascalCase"))

    def test_pascal_case_namespaced_no_warning(self):
        result = _analyse('pypi::RunJob')
        self.assertFalse(_has_warning_containing(result, "PascalCase"))

    def test_snake_case_namespaced_task_warns(self):
        result = _analyse('pypi::run_job')
        self.assertTrue(_has_warning_containing(result, "run_job"))

    def test_meta_task_template_snake_case_warns(self):
        result = _analyse('MAP<fetch_data>')
        self.assertTrue(_has_warning_containing(result, "fetch_data"))
        self.assertTrue(_has_warning_containing(result, "PascalCase"))

    def test_meta_task_template_pascal_no_warning(self):
        result = _analyse('MAP<FetchData>')
        self.assertFalse(_has_warning_containing(result, "PascalCase"))

    def test_meta_task_namespaced_template_snake_warns(self):
        result = _analyse('FILTER<pypi::enrich_data>')
        self.assertTrue(_has_warning_containing(result, "enrich_data"))

    def test_all_uppercase_passes_convention(self):
        # All-uppercase is treated as passing (acronym / legacy name)
        result = _analyse('WORKER')
        self.assertFalse(_has_warning_containing(result, "PascalCase"))

    def test_multiple_non_pascal_tasks_in_chain(self):
        result = _analyse('fetch_data -> process_result')
        pascal_warnings = [
            d for d in result.warnings if "PascalCase" in d.message
        ]
        self.assertEqual(len(pascal_warnings), 2)

    def test_non_pascal_task_warning_level_is_warning_not_error(self):
        result = _analyse('fetch_data')
        self.assertFalse(result.has_errors)
        self.assertTrue(result.has_warnings)
        w = result.warnings[0]
        self.assertEqual(w.level, SemanticLevel.WARNING)


# ===========================================================================
# SemanticResult bookkeeping
# ===========================================================================

class TestSemanticResult(unittest.TestCase):
    """Verify the SemanticResult container itself."""

    def test_has_errors_false_when_clean(self):
        result = _analyse('FetchData')
        self.assertFalse(result.has_errors)

    def test_has_errors_true_on_duplicate_attr(self):
        result = _analyse('Worker[opt = 1, opt = 2]')
        self.assertTrue(result.has_errors)

    def test_all_combines_errors_and_warnings(self):
        # Trigger both an error (duplicate attr) and a warning (non-PascalCase)
        result = _analyse('fetch_data[opt = 1, opt = 2]')
        self.assertTrue(len(result.all) >= 2)
        levels = {d.level for d in result.all}
        self.assertIn(SemanticLevel.ERROR, levels)
        self.assertIn(SemanticLevel.WARNING, levels)

    def test_diagnostic_str(self):
        result = _analyse('Worker[opt = 1, opt = 2]')
        d = result.errors[0]
        s = str(d)
        self.assertIn("[ERROR]", s)
        self.assertIn("opt", s)

    def test_bool_false_when_has_diagnostics(self):
        result = _analyse('fetch_data')
        self.assertFalse(bool(result))

    def test_bool_true_when_clean(self):
        result = _analyse('FetchData')
        self.assertTrue(bool(result))


if __name__ == "__main__":
    unittest.main()

