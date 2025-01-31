__all__ = ["pointy_parser"]

import lexer
from ply.yacc import yacc

pointy_lexer = lexer.PointyLexer()
tokens = pointy_lexer.tokens


def p_expression(p):
    """
    expression : term POINTER term
                | term PPOINTER term
                | expression POINTER term
                | expression PPOINTER term
                | descriptor POINTER term
                | descriptor PPOINTER term
    """
    p[0] = (p[2], p[1], p[3])


def p_expression_term(p):
    """
    expression : term
    """
    p[0] = p[1]


def p_task(p):
    """
    term : task
    """
    p[0] = p[1]


def p_descriptor(p):
    """
    descriptor : DESCRIPTOR
    """
    p[0] = ("DESCRIPTOR", p[1])


def p_task_taskname(p):
    """
    task : TASKNAME
    """
    p[0] = ("TASKNAME", p[1])


def p_task_grouped(p):
    """
    task :  task LPAREN expression SEPERATOR expression RPAREN
    """
    p[0] = ("GROUP", p[1], p[3], p[5])


# Error rule for syntax errors
def p_error(p):
    raise SyntaxError(f"Syntax error in input {p}!")


parser = yacc()


def pointy_parser(code: str):
    return parser.parse(code, lexer=pointy_lexer.lexer)
