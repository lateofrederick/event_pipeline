import unittest

from volnux.parser import TernaryExprNode, IndexExprNode, VariableAccessNode
from volnux.parser.ast import BinOpNode, ConditionalNode, TaskNode, MetaEventNode, MapNode
from volnux.parser.grammar_v2 import pointy_parser



class TestProgram(unittest.TestCase):
    def test_empty_program(self):
        program = pointy_parser("")
        self.assertIsNotNone(program)
        self.assertEqual(program.directives, {})
        self.assertEqual(program.global_variables, {})
        self.assertIsNone(program.chain)

    def test_program_with_directives_and_variables(self):
        program = pointy_parser('@mode:"CFG" @version:1.0 @foo=42')
        self.assertIn("mode", program.directives)
        self.assertIn("version", program.directives)
        self.assertIn("foo", program.global_variables)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.directives["version"].value, 1.0)
        self.assertEqual(program.global_variables["foo"].value, 42)

    def test_program_with_chain(self):
        program = pointy_parser('TaskA -> TaskB')
        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '->')
        self.assertEqual(program.chain.left.task, 'TaskA')
        self.assertEqual(program.chain.right.task, 'TaskB')

    def test_program_with_directives_variables_and_chain(self):
        program = pointy_parser('@mode:"CFG" @foo=42 TaskA -> TaskB')
        self.assertIn("mode", program.directives)
        self.assertIn("foo", program.global_variables)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.global_variables["foo"].value, 42)
        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '->')
        self.assertEqual(program.chain.left.task, 'TaskA')
        self.assertEqual(program.chain.right.task, 'TaskB')

    def test_simple_conditional_chain_with_directive(self):
        program = pointy_parser(
            """
            @mode:"CFG"
            
            StartProcess -> EvaluateCondition (
                0 -> EndProcess,
                1 -> PerformWork -> EvaluateCondition  # Cycle back
            )
            """
        )

        self.assertIn("mode", program.directives)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '->')
        self.assertEqual(program.chain.left.task, 'StartProcess')
        conditional = program.chain.right
        self.assertEqual(conditional.task.task, 'EvaluateCondition')
        self.assertEqual(len(conditional.branches.statements), 2)
        branches = {a.condition.value: a for a in conditional.branches.statements}
        self.assertEqual(branches[0].operator, '->')
        self.assertEqual(branches[0].task.task, 'EndProcess')
        self.assertEqual(branches[1].operator, '->')
        self.assertIsInstance(branches[1].task, BinOpNode)
        self.assertEqual(branches[1].task.op, '->')
        self.assertEqual(branches[1].task.left.task, 'PerformWork')
        self.assertEqual(branches[1].task.right.task, 'EvaluateCondition')

    def test_nested_conditional_workflow_with_directives(self):
        program = pointy_parser(
            """
                @mode:"CFG"
                @recursive_depth:5000
                
                StartJob -> ProcessBatch (
                    0 -> LogError -> NotifyAdmin,
                    1 -> ValidateResults (
                        0 -> CorrectData -> ProcessBatch,  # Loop back for retry
                        1 -> FinalizeJob
                    )
                )
            """
        )

        self.assertIn("mode", program.directives)
        self.assertIn("recursive_depth", program.directives)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.directives["recursive_depth"].value, 5000)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '->')
        self.assertEqual(program.chain.left.task, 'StartJob')
        conditional = program.chain.right
        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, 'ProcessBatch')
        self.assertEqual(len(conditional.branches.statements), 2)
        branches = {a.condition.value: a for a in conditional.branches.statements}

        # Branch 0
        self.assertEqual(branches[0].operator, '->')
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, '->')
        self.assertEqual(branches[0].task.left.task, 'LogError')
        self.assertEqual(branches[0].task.right.task, 'NotifyAdmin')

        # Branch 1
        self.assertEqual(branches[1].operator, '->')
        self.assertIsInstance(branches[1].task, ConditionalNode)
        nested_conditional = branches[1].task
        self.assertEqual(nested_conditional.task.task, 'ValidateResults')
        self.assertEqual(len(nested_conditional.branches.statements), 2)
        nested_branches = {a.condition.value: a for a in nested_conditional.branches.statements}

        # Nested Branch 0
        self.assertEqual(nested_branches[0].operator, '->')
        self.assertIsInstance(nested_branches[0].task, BinOpNode)
        self.assertEqual(nested_branches[0].task.op, '->')
        self.assertEqual(nested_branches[0].task.left.task, 'CorrectData')
        self.assertEqual(nested_branches[0].task.right.task, 'ProcessBatch')

        # Nested Branch 1
        self.assertEqual(nested_branches[1].operator, '->')
        self.assertIsInstance(nested_branches[1].task, TaskNode)
        self.assertEqual(nested_branches[1].task.task, 'FinalizeJob')

    def test_triple_nested_conditional_workflow_with_directives(self):
        program = pointy_parser(
            """
            @mode:"DAG"
            @recursive_depth:3000
            
            Level1 -> Level2 (
                0 -> ErrorPath1 -> ErrorPath2 -> ErrorPath3,
                1 -> Level3 (
                    0 -> FallbackA -> FallbackB,
                    1 -> Level4 (
                        0 -> RecoveryFlow,
                        1 -> Level5 -> Level6 -> Level7
                    )
                )
            )
            """
        )

        self.assertIn("mode", program.directives)
        self.assertIn("recursive_depth", program.directives)
        self.assertEqual(program.directives["mode"].value, "DAG")
        self.assertEqual(program.directives["recursive_depth"].value, 3000)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '->')
        self.assertEqual(program.chain.left.task, 'Level1')
        conditional = program.chain.right
        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, 'Level2')
        self.assertEqual(len(conditional.branches.statements), 2)
        branches = {a.condition.value: a for a in conditional.branches.statements}

        # Branch 0
        self.assertEqual(branches[0].operator, '->')
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, '->')
        self.assertIsInstance(branches[0].task.left, BinOpNode)
        self.assertIsInstance(branches[0].task.left.left, TaskNode)
        self.assertEqual(branches[0].task.left.left.task, 'ErrorPath1')
        self.assertIsInstance(branches[0].task.left.right, TaskNode)
        self.assertEqual(branches[0].task.left.right.task, 'ErrorPath2')
        self.assertIsInstance(branches[0].task.right, TaskNode)
        self.assertEqual(branches[0].task.right.task, 'ErrorPath3')


        # Branch 1
        self.assertEqual(branches[1].operator, '->')
        self.assertIsInstance(branches[1].task, ConditionalNode)
        level3_conditional = branches[1].task
        self.assertEqual(level3_conditional.task.task, 'Level3')
        self.assertEqual(len(level3_conditional.branches.statements), 2)
        level3_branches = {a.condition.value: a for a in level3_conditional.branches.statements}

        # Level3 Branch 0
        self.assertEqual(level3_branches[0].operator, '->')
        self.assertIsInstance(level3_branches[0].task, BinOpNode)
        self.assertEqual(level3_branches[0].task.op, '->')
        self.assertEqual(level3_branches[0].task.left.task, 'FallbackA')
        self.assertEqual(level3_branches[0].task.right.task, 'FallbackB')

        # Level3 Branch 1
        self.assertEqual(level3_branches[1].operator, '->')
        self.assertIsInstance(level3_branches[1].task, ConditionalNode)
        level4_conditional = level3_branches[1].task
        self.assertEqual(level4_conditional.task.task, 'Level4')
        self.assertEqual(len(level4_conditional.branches.statements), 2)
        level4_branches = {a.condition.value: a for a in level4_conditional.branches.statements}

        # Level4 Branch 0
        self.assertEqual(level4_branches[0].operator, '->')
        self.assertIsInstance(level4_branches[0].task, TaskNode)
        self.assertEqual(level4_branches[0].task.task, 'RecoveryFlow')

        # Level4 Branch 1
        self.assertEqual(level4_branches[1].operator, '->')
        self.assertIsInstance(level4_branches[1].task, BinOpNode)
        self.assertEqual(level4_branches[1].task.op, '->')
        self.assertIsInstance(level4_branches[1].task.left, BinOpNode)
        self.assertEqual(level4_branches[1].task.left.op, '->')
        self.assertIsInstance(level4_branches[1].task.left.left, TaskNode)
        self.assertEqual(level4_branches[1].task.left.left.task, 'Level5')
        self.assertIsInstance(level4_branches[1].task.left.right, TaskNode)
        self.assertEqual(level4_branches[1].task.left.right.task, 'Level6')
        self.assertIsInstance(level4_branches[1].task.right, TaskNode)
        self.assertEqual(level4_branches[1].task.right.task, 'Level7')

    def test_multi_namespace_workflow_with_directives(self):
        program = pointy_parser(
            """
            @mode:"DAG"
            @recursive_depth:1500
            
            local::FetchOrders 
                -> pypi::ValidateOrders 
                -> github::EnrichWithCustomerData || local::CalculateTotals |-> pypi::GenerateInvoice
            """
        )

        self.assertIn("mode", program.directives)
        self.assertIn("recursive_depth", program.directives)
        self.assertEqual(program.directives["mode"].value, "DAG")
        self.assertEqual(program.directives["recursive_depth"].value, 1500)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '|->')
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.namespace, "pypi")
        self.assertEqual(program.chain.right.task, 'GenerateInvoice')

        self.assertIsInstance(program.chain.left, BinOpNode)
        self.assertEqual(program.chain.left.op, '||')
        self.assertIsInstance(program.chain.left.right, TaskNode)
        self.assertEqual(program.chain.left.right.namespace, "local")
        self.assertEqual(program.chain.left.right.task, 'CalculateTotals')

        self.assertIsInstance(program.chain.left.left, BinOpNode)
        self.assertEqual(program.chain.left.left.op, '->')
        self.assertIsInstance(program.chain.left.left.right, TaskNode)
        self.assertEqual(program.chain.left.left.right.namespace, "github")
        self.assertEqual(program.chain.left.left.right.task, 'EnrichWithCustomerData')

        self.assertIsInstance(program.chain.left.left.left, BinOpNode)
        self.assertEqual(program.chain.left.left.left.op, '->')
        self.assertIsInstance(program.chain.left.left.left.left, TaskNode)
        self.assertEqual(program.chain.left.left.left.left.namespace, "local")
        self.assertEqual(program.chain.left.left.left.left.task, 'FetchOrders')

        self.assertIsInstance(program.chain.left.left.left.right, TaskNode)
        self.assertEqual(program.chain.left.left.left.right.namespace, "pypi")
        self.assertEqual(program.chain.left.left.left.right.task, 'ValidateOrders')

    def test_variables_with_meta_events(self):
        program = pointy_parser(
            """
            @parallel_workers = 5
            @batch_config = {"size": 100, "timeout": 30}
            @enable_retry = true
            
            ConfigureSystem[worker_count=$parallel_workers] ->
            FetchWorkQueue |->
            MAP<ProcessWorkItem>[
                batch_size=$batch_config["size"],
                concurrent=true,
                retries=$enable_retry ? 3 : 0
            ] |->
            UpdateMetrics
            """
        )

        self.assertIn("parallel_workers", program.global_variables)
        self.assertIn("batch_config", program.global_variables)
        self.assertIn("enable_retry", program.global_variables)
        self.assertEqual(program.global_variables["parallel_workers"].value, 5)
        self.assertIsInstance(program.global_variables["batch_config"], MapNode)
        self.assertEqual(program.global_variables["batch_config"].value["size"].value, 100)
        self.assertEqual(program.global_variables["batch_config"].value["timeout"].value, 30)
        self.assertEqual(program.global_variables["enable_retry"].value, True)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, '|->')
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, 'UpdateMetrics')

        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, '|->')
        self.assertIsInstance(left_chain.right, MetaEventNode)
        self.assertEqual(left_chain.right.mode, 'MAP')
        self.assertEqual(left_chain.right.template_event, 'ProcessWorkItem')
        self.assertEqual(left_chain.right.template_event_namespace, 'local')
        self.assertIsInstance(left_chain.right.options, list)
        options = {opt.attr: opt.value for opt in left_chain.right.options}
        self.assertIn('batch_size', options)
        self.assertIn('concurrent', options)
        self.assertIn('retries', options)
        self.assertIsInstance(options['batch_size'], IndexExprNode)
        self.assertEqual(options['concurrent'].value, True)
        self.assertIsInstance(options['retries'], TernaryExprNode)
        self.assertIsInstance(options['retries'].condition, VariableAccessNode)
        self.assertEqual(options['retries'].true_expr.value, 3)
        self.assertEqual(options['retries'].false_expr.value, 0)

        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, '->')
        self.assertIsInstance(left_chain.left.left, TaskNode)
        self.assertEqual(left_chain.left.left.task, 'ConfigureSystem')
        self.assertIsInstance(left_chain.left.left.options, list)
        self.assertEqual(len(left_chain.left.left.options), 1)
        option = left_chain.left.left.options[0]
        self.assertEqual(option.attr, 'worker_count')
        self.assertIsInstance(option.value, VariableAccessNode)
        self.assertEqual(option.value.name, 'parallel_workers')
        self.assertIsInstance(left_chain.left.right, TaskNode)
        self.assertEqual(left_chain.left.right.task, 'FetchWorkQueue')






if __name__ == '__main__':
    unittest.main()
