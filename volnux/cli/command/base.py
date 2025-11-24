import argparse
import logging
import sys
import typing
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Optional

from volnux.registry import Registry

from .style import Style

_command_registry = Registry()


logger = logging.getLogger(__name__)


def get_command_registry():
    return _command_registry


class CommandError(Exception):
    """Exception raised for command errors."""

    pass


class CommandCategory(Enum):
    PROJECT_MANAGEMENT = "Project Management"
    WORKFLOW_MANAGEMENT = "Workflow Management"
    EXECUTION = "Execution"
    DEVELOPMENT = "Development"
    HELP = "Help"
    OTHER = "Other"


class CommandMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace, **kwargs):
        """
        Called when a new class is created.
        Automatically registers the class with the global registry.
        """
        cls = super().__new__(mcs, name, bases, namespace)

        # Register it if it's not the base class
        if name != "BaseCommand" and any(
            isinstance(base, CommandMeta) for base in bases
        ):
            try:
                _command_registry.register(cls, getattr(cls, "name", None))
            except RuntimeError as e:
                logger.warning(str(e))

        return cls


def get_commands_by_category(category: CommandCategory) -> typing.List["BaseCommand"]:
    """
    Return commands registered for the given category.
    Args:
        category (CommandCategory): Category to look up commands for.
    Returns:
         List["BaseCommand"]: Commands registered for the given category.
    """
    commands = []
    for command in _command_registry.list_all_classes():
        command = typing.cast(BaseCommand, command)
        if command.category == category:
            if command not in commands:
                commands.append(command)

    return commands


class BaseCommand(metaclass=CommandMeta):
    """
    Base class for all Volnux commands.
    Similar to Django's BaseCommand.
    """

    help = ""

    # The name to use for this command. If not provided, it uses the class name
    name = None

    # command category
    category = CommandCategory.OTHER

    def __init__(self):
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        self.style = Style()

    def create_parser(self, prog_name: str, subcommand: str) -> argparse.ArgumentParser:
        """
        Create and return the ArgumentParser for this command.
        """
        parser = argparse.ArgumentParser(
            prog=f"{prog_name} {subcommand}",
            description=self.help or None,
        )
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Entry point for subclassed commands to add custom arguments.
        """
        pass

    def print_help(self, prog_name: str, subcommand: str) -> None:
        """
        Print the help message for this command.
        """
        parser = self.create_parser(prog_name, subcommand)
        parser.print_help()

    def execute(self, *args, **options) -> None:
        """
        Execute the command.
        """
        try:
            output = self.handle(*args, **options)
            if output:
                self.stdout.write(output)
        except CommandError as e:
            self.stderr.write(self.style.ERROR(f"Error: {e}"))
            sys.exit(1)
        except KeyboardInterrupt:
            self.stderr.write(self.style.WARNING("\nOperation cancelled."))
            sys.exit(1)

    @abstractmethod
    def handle(self, *args, **options) -> Optional[str]:
        """
        The actual logic of the command. Subclasses must implement this.
        """
        pass

    def success(self, message: str) -> None:
        """Write a success message."""
        self.stdout.write(self.style.SUCCESS(message))

    def warning(self, message: str) -> None:
        """Write a warning message."""
        self.stdout.write(self.style.WARNING(message))

    def error(self, message: str) -> None:
        """Write an error message."""
        self.stderr.write(self.style.ERROR(message))
