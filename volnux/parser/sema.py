"""
Semantic analyser for the Pointy language.

Walks a parsed ProgramNode AST and emits SemanticDiagnostic objects for:

  Errors
  ------
  - Duplicate attribute names inside a task / meta-task / grouping option list.
  - Duplicate descriptor values across branches of a ConditionalNode.
  - Unknown directive names (not in the recognised set).

  Warnings
  --------
  - Unknown option key — key not in Options known fields (goes to extras).
  - Known option key with a wrong literal value type (type mismatch).
  - Variable declared with @var but never referenced via $var.
  - Task / meta-task template name that is not PascalCase (recommendation).

Parser-level constraints (retry >= 2, descriptor 0-9) are intentionally left
in the parser and are NOT duplicated here.
"""
import typing

from .ast import (
    ASTNode,
    AttributeNode,
    BinOpNode,
    BranchNode,
    ComparisonExprNode,
    ConditionalNode,
    DescriptorNode,
    DirectiveNode,
    EnvironmentVariableAccessNode,
    IndexExprNode,
    ListNode,
    LiteralNode,
    LiteralType,
    MapNode,
    MetaTaskNode,
    NullCoalesceExprNode,
    PipelineGroupingNode,
    ProgramNode,
    RetryNode,
    TaskNode,
    TernaryExprNode,
    UnaryOpNode,
    VariableAccessNode,
    VariableDeclNode,
)
from .sema_errors import SemanticDiagnostic, SemanticLevel, SemanticResult
from .visitor import ASTVisitorInterface

# ---------------------------------------------------------------------------
# Static metadata used by the analyser
# ---------------------------------------------------------------------------

# Known directive names accepted by the Pointy runtime.
_KNOWN_DIRECTIVES: typing.FrozenSet[str] = frozenset({"mode", "recursive-depth"})

# Mapping of known Options field names to their acceptable raw Python types.
# Complex fields (executor_config, extras) are skipped — their values are
# always dict-like and don't have a single "wrong" literal type to warn about.
_OPTIONS_FIELD_TYPES: typing.Dict[str, typing.Tuple[type, ...]] = {
    "retry_attempts": (int,),
    "executor": (str,),
    "result_evaluation_strategy": (str,),
    "stop_condition": (str,),
    "bypass_event_checks": (bool,),
}

_OPTIONS_KNOWN_FIELDS: typing.FrozenSet[str] = frozenset(
    {
        "retry_attempts",
        "executor",
        "executor_config",
        "extras",
        "result_evaluation_strategy",
        "stop_condition",
        "bypass_event_checks",
    }
)

# ---------------------------------------------------------------------------
# Expression-level operator classification
# ---------------------------------------------------------------------------

# Pipeline operators — never subject to expression-level type checks.
_PIPELINE_OPS: typing.FrozenSet[str] = frozenset({"->", "|->"})

# Bitwise and shift operators — both operands must be integers.
_BITWISE_OPS: typing.FrozenSet[str] = frozenset({"|", "&", "^", "<<", ">>", ">>>"})

# Arithmetic operators — operands must be numeric (not string or bool).
_ARITHMETIC_OPS: typing.FrozenSet[str] = frozenset({"+", "-", "*", "/", "%"})

# LiteralTypes that represent string-like values.
_STRING_TYPES: typing.FrozenSet[LiteralType] = frozenset(
    {LiteralType.STRING, LiteralType.IMPORT_STRING}
)


def _is_pascal_case(name: str) -> bool:
    """Return True when *name* looks like PascalCase.

    Heuristic: first character is uppercase and there are no underscores.
    All-uppercase names (acronyms) also pass.
    """
    return bool(name) and name[0].isupper() and "_" not in name


# ---------------------------------------------------------------------------
# Sema visitor
# ---------------------------------------------------------------------------


class Sema(ASTVisitorInterface):
    """Semantic analyser — visitor over a Pointy AST."""

    def __init__(self) -> None:
        self._result: SemanticResult = SemanticResult()
        # Names declared via @var = ...
        self._declared_vars: typing.Set[str] = set()
        # Names actually referenced via $var inside the chain
        self._referenced_vars: typing.Set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyse(self, node: "ProgramNode") -> SemanticResult:
        """Run the semantic pass over *node* and return collected diagnostics."""
        self._result = SemanticResult()
        self._declared_vars = set()
        self._referenced_vars = set()
        self._visit(node)
        return self._result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _err(self, message: str, node: typing.Optional[ASTNode] = None) -> None:
        self._result.add(SemanticDiagnostic(SemanticLevel.ERROR, message, node))

    def _warn(self, message: str, node: typing.Optional[ASTNode] = None) -> None:
        self._result.add(SemanticDiagnostic(SemanticLevel.WARNING, message, node))

    def _visit(self, node: typing.Optional[ASTNode]) -> None:
        if node is not None:
            node.accept(self)

    def _check_options(
        self,
        options: typing.List[AttributeNode],
        context: str,
    ) -> None:
        """Single-pass check over an attribute list.

        Checks performed:
          1. Duplicate attribute names → error.
          2. Unknown option key → warning.
          3. Known key with wrong literal type → warning.
          4. Recursively visit attribute values (for variable-reference tracking).
        """
        seen: typing.Dict[str, bool] = {}
        for attr in options:
            name = attr.attr

            # 1. Duplicate attribute names
            if name in seen:
                self._err(
                    f"Duplicate attribute '{name}' in '{context}'",
                    attr,
                )
            else:
                seen[name] = True

            # 2. Unknown option key
            if name not in _OPTIONS_KNOWN_FIELDS:
                self._warn(
                    f"Unknown option '{name}' on '{context}'. "
                    f"It will be placed in extras.",
                    attr,
                )
            else:
                # 3. Type mismatch for known keys with a simple expected type
                expected = _OPTIONS_FIELD_TYPES.get(name)
                if expected is not None:
                    value_node = attr.value
                    if (
                        isinstance(value_node, LiteralNode)
                        and value_node.value is not None
                    ):
                        actual = value_node.value
                        actual_type = type(actual)
                        if not isinstance(actual, expected):
                            self._warn(
                                f"Option '{name}' on '{context}' expects "
                                f"{' or '.join(t.__name__ for t in expected)}, "
                                f"got {actual_type.__name__}",
                                attr,
                            )

            # 4. Recurse into the value expression
            self._visit(attr.value)

    # ------------------------------------------------------------------
    # Visitor implementations
    # ------------------------------------------------------------------

    def visit_program(self, node: "ProgramNode") -> None:
        # Collect declared variable names for unused-variable tracking
        for var_name in node.global_variables:
            self._declared_vars.add(var_name)

        # Validate directive names
        for directive_name in node.directives:
            if directive_name not in _KNOWN_DIRECTIVES:
                self._err(
                    f"Unknown directive '@{directive_name}'. "
                    f"Known directives: {', '.join(sorted(_KNOWN_DIRECTIVES))}",
                    node,
                )

        # Walk the workflow chain
        if node.chain is not None:
            self._visit(node.chain)

        # Unused variable warnings — emitted after the full walk so that all
        # references have been collected
        for var_name in self._declared_vars:
            if var_name not in self._referenced_vars:
                self._warn(
                    f"Variable '{var_name}' is declared but never referenced",
                    node,
                )

    def visit_task(self, node: "TaskNode") -> None:
        # PascalCase recommendation
        if not _is_pascal_case(node.task):
            self._warn(
                f"Task name '{node.task}' is not PascalCase "
                f"(recommendation: use PascalCase)",
                node,
            )

        if node.options:
            self._check_options(node.options, node.fully_qualified_name)

    def visit_meta_task(self, node: "MetaTaskNode") -> None:
        ns = node.template_event_namespace
        template = node.template_task
        label = (
            f"{node.mode}<{ns}::{template}>"
            if ns and ns != "local"
            else f"{node.mode}<{template}>"
        )

        # PascalCase recommendation on template name
        if not _is_pascal_case(template):
            self._warn(
                f"Meta-task template name '{template}' is not PascalCase "
                f"(recommendation: use PascalCase)",
                node,
            )

        if node.options:
            self._check_options(node.options, label)

    def visit_pipeline_grouping(self, node: "PipelineGroupingNode") -> None:
        for expr in node.expressions:
            self._visit(expr)

        if node.options:
            self._check_options(node.options, "grouped expression")

    def visit_conditional(self, node: "ConditionalNode") -> None:
        self._visit(node.task)

        seen_descriptors: typing.Dict[int, bool] = {}
        task_label = (
            node.task.task
            if isinstance(node.task, TaskNode)
            else repr(node.task)
        )

        for branch in node.branches:
            desc_val = branch.condition.value
            if desc_val in seen_descriptors:
                self._err(
                    f"Duplicate descriptor '{desc_val}' in conditional "
                    f"on '{task_label}'",
                    branch,
                )
            else:
                seen_descriptors[desc_val] = True

            self._visit(branch.task)

    def visit_binop(self, node: "BinOpNode") -> None:
        self._visit(node.left)
        self._visit(node.right)

        op = node.op

        # Pipeline operators carry no expression-level type meaning.
        if op in _PIPELINE_OPS:
            return

        left_lit = node.left if isinstance(node.left, LiteralNode) else None
        right_lit = node.right if isinstance(node.right, LiteralNode) else None

        # --- Division by zero ---
        if op == "/" and right_lit is not None:
            if right_lit.type == LiteralType.NUMBER and right_lit.value == 0:
                self._err("Division by zero", node)

        # --- Modulo by zero ---
        if op == "%" and right_lit is not None:
            if right_lit.type == LiteralType.NUMBER and right_lit.value == 0:
                self._err("Modulo by zero", node)

        # --- Arithmetic on string or boolean literal ---
        if op in _ARITHMETIC_OPS:
            for lit, side in ((left_lit, "left"), (right_lit, "right")):
                if lit is None:
                    continue
                if lit.type in _STRING_TYPES:
                    self._warn(
                        f"Arithmetic operator '{op}': {side} operand is a string "
                        f"literal ({lit.value!r}); strings are not numeric",
                        node,
                    )
                elif lit.type == LiteralType.BOOLEAN:
                    self._warn(
                        f"Arithmetic operator '{op}': {side} operand is a boolean "
                        f"literal ({lit.value!r}); booleans are not numeric",
                        node,
                    )

        # --- Constant logical short-circuit ---
        # false && x  or  x && false  →  expression is always false
        if op == "&&":
            for lit, side in ((left_lit, "left"), (right_lit, "right")):
                if (
                    lit is not None
                    and lit.type == LiteralType.BOOLEAN
                    and lit.value is False
                ):
                    self._warn(
                        f"Logical AND '&&': {side} operand is a constant 'false'; "
                        f"the expression is always false",
                        node,
                    )

        # true || x  or  x || true  →  expression is always true
        # (only inside attribute expressions; pipeline || has TaskNode children, not LiteralNodes)
        if op == "||":
            for lit, side in ((left_lit, "left"), (right_lit, "right")):
                if (
                    lit is not None
                    and lit.type == LiteralType.BOOLEAN
                    and lit.value is True
                ):
                    self._warn(
                        f"Logical OR '||': {side} operand is a constant 'true'; "
                        f"the expression is always true",
                        node,
                    )

        # --- Bitwise / shift on float or string literal ---
        if op in _BITWISE_OPS:
            for lit, side in ((left_lit, "left"), (right_lit, "right")):
                if lit is None:
                    continue
                if lit.type == LiteralType.NUMBER and isinstance(lit.value, float):
                    self._warn(
                        f"Bitwise/shift operator '{op}': {side} operand is a float "
                        f"literal ({lit.value!r}); bitwise operations require integers",
                        node,
                    )
                elif lit.type in _STRING_TYPES:
                    self._warn(
                        f"Bitwise/shift operator '{op}': {side} operand is a string "
                        f"literal ({lit.value!r}); bitwise operations require integers",
                        node,
                    )

    def visit_unaryop(self, node: "UnaryOpNode") -> None:
        self._visit(node.right)

        # Bitwise NOT on a non-integer literal is always wrong.
        if node.op == "~" and isinstance(node.right, LiteralNode):
            lit = node.right
            if lit.type == LiteralType.NUMBER and isinstance(lit.value, float):
                self._warn(
                    f"Bitwise NOT '~' applied to a float literal ({lit.value!r}); "
                    f"bitwise operations require integers",
                    node,
                )
            elif lit.type in _STRING_TYPES:
                self._warn(
                    f"Bitwise NOT '~' applied to a string literal ({lit.value!r}); "
                    f"bitwise operations require integers",
                    node,
                )

        # Unary minus on a string or boolean literal is meaningless.
        if node.op == "-" and isinstance(node.right, LiteralNode):
            lit = node.right
            if lit.type in _STRING_TYPES:
                self._warn(
                    f"Unary minus '-' applied to a string literal ({lit.value!r}); "
                    f"strings are not numeric",
                    node,
                )
            elif lit.type == LiteralType.BOOLEAN:
                self._warn(
                    f"Unary minus '-' applied to a boolean literal ({lit.value!r}); "
                    f"booleans are not numeric",
                    node,
                )

        # Logical NOT on a non-boolean literal is likely a mistake.
        if node.op == "!" and isinstance(node.right, LiteralNode):
            lit = node.right
            if lit.type == LiteralType.NUMBER:
                self._warn(
                    f"Logical NOT '!' applied to a numeric literal ({lit.value!r}); "
                    f"expected a boolean operand",
                    node,
                )
            elif lit.type in _STRING_TYPES:
                self._warn(
                    f"Logical NOT '!' applied to a string literal ({lit.value!r}); "
                    f"expected a boolean operand",
                    node,
                )

    def visit_retry(self, node: "RetryNode") -> None:
        self._visit(node.job)
        self._visit(node.attempts)

    def visit_literal(self, node: "LiteralNode") -> None:
        pass  # leaf node, nothing to check

    def visit_descriptor(self, node: "DescriptorNode") -> None:
        pass  # range already enforced by the parser

    def visit_variable_access(self, node: "VariableAccessNode") -> None:
        self._referenced_vars.add(node.name)

    def visit_access_environment_variable(
        self, node: "EnvironmentVariableAccessNode"
    ) -> None:
        pass  # env vars are not user-declared, skip tracking

    def visit_variable_declaration(self, node: "VariableDeclNode") -> None:
        pass  # declarations tracked via ProgramNode.global_variables in visit_program

    def visit_directive(self, node: "DirectiveNode") -> None:
        pass  # directive name validation done in visit_program

    def visit_null_coalesce(self, node: "NullCoalesceExprNode") -> None:
        self._visit(node.left)
        self._visit(node.right)

        # A known non-null constant on the left means the right branch is dead.
        if (
            isinstance(node.left, LiteralNode)
            and node.left.type != LiteralType.NULL
        ):
            self._warn(
                f"Left operand of '??' is a non-null literal; "
                f"the right branch is unreachable",
                node,
            )

    def visit_comparison_expr(self, node: "ComparisonExprNode") -> None:
        self._visit(node.left)
        self._visit(node.right)

        # Ordering comparisons (<, >, <=, >=) between a string and a number
        # literal are almost certainly a mistake. Equality (==, !=) is fine.
        if node.operator in {"<", ">", "<=", ">="}:
            left_lit = node.left if isinstance(node.left, LiteralNode) else None
            right_lit = node.right if isinstance(node.right, LiteralNode) else None
            if left_lit is not None and right_lit is not None:
                left_str = left_lit.type in _STRING_TYPES
                right_str = right_lit.type in _STRING_TYPES
                left_num = left_lit.type == LiteralType.NUMBER
                right_num = right_lit.type == LiteralType.NUMBER
                if (left_str and right_num) or (left_num and right_str):
                    self._warn(
                        f"Ordering comparison '{node.operator}' between a string "
                        f"literal and a number literal is likely a mistake",
                        node,
                    )

    def visit_branch(self, node: "BranchNode") -> None:
        # Branches are visited directly in visit_conditional; this is a no-op
        # fallback in case accept() is called independently.
        self._visit(node.task)

    def visit_index_expr(self, node: typing.Any) -> None:
        # IndexExprNode.accept() has a known bug where it passes `visitor`
        # instead of `self`, so `node` here may be the visitor itself.
        # Guard defensively.
        if isinstance(node, IndexExprNode):
            self._visit(node.collection)
            self._visit(node.index)

    def visit_attribute(self, node: "AttributeNode") -> None:
        # Attributes are checked via _check_options; this is a fallback.
        self._visit(node.value)

    def visit_list(self, node: "ListNode") -> None:
        for item in node.value:
            self._visit(item)

    def visit_map(self, node: "MapNode") -> None:
        for value in node.value.values():
            self._visit(value)

    # TernaryExprNode is not in ASTVisitorInterface but TernaryExprNode.accept
    # calls visitor.visit_ternary_expr, so we must define it here.
    def visit_ternary_expr(self, node: "TernaryExprNode") -> None:
        self._visit(node.condition)
        self._visit(node.true_expr)
        self._visit(node.false_expr)

        # A constant literal as the condition means one branch is always dead.
        if isinstance(node.condition, LiteralNode):
            self._warn(
                f"Ternary condition is a constant literal; "
                f"one branch is always unreachable",
                node,
            )

