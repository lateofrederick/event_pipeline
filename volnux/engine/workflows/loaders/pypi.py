import importlib
import logging
import subprocess
import sys
import typing

from volnux import Event
from volnux.base import EventType

from .utils import get_workflow_config_name

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ..registry import WorkflowRegistry


class LoadFromPyPi(Event):
    name = "pypi"

    event_type = EventType.SYSTEM

    def _load_pypi_workflow(
        self, package_name: str, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        """Load a workflow from an installed PyPI package."""
        from ..workflow import WorkflowConfig

        loading_status = False
        workflow_config_class = None

        try:
            # Import the workflow module
            # Convention: package exports WorkflowConfig in __init__.py or workflow.py
            module = importlib.import_module(package_name)

            workflow_config_class_name = get_workflow_config_name(package_name)

            # Check if module has a 'workflow' attribute or submodule
            if hasattr(module, workflow_config_class_name):
                workflow_config_class = getattr(module, workflow_config_class_name)
            elif hasattr(module, "workflow"):
                workflow_module = module.workflow
                for attr_name in dir(workflow_module):
                    attr_name = get_workflow_config_name(attr_name)
                    attr = getattr(workflow_module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, WorkflowConfig)
                        and attr != WorkflowConfig
                    ):
                        workflow_config_class = attr
                        break

            if workflow_config_class:
                workflow_config = workflow_config_class()
                registry.register(workflow_config)
                loading_status = True
                logger.info(f"  ✓ Loaded workflow from PyPI: {workflow_config.name}")
            else:
                logger.error(f"  ✗ No WorkflowConfig found in {package_name}")
                loading_status = False

        except ImportError as e:
            logger.error(f"  ✗ Failed to import {package_name}: {e}")
            loading_status = False

        return loading_status, workflow_config_class

    def process(
        self, version: str, package_name: str, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        """
        Install a workflow from PyPI.

        Example:
            registry.install_from_pypi('workflow-docker-registry', version='1.0.0')
        """

        print(f"Installing workflow from PyPI: {package_name}")

        # Build pip install command
        if version:
            package_spec = f"{package_name}=={version}"
        else:
            package_spec = package_name

        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", package_spec]
            )
            logger.info(f"  ✓ Installed {package_spec}")

            # Try to import and register the workflow
            return self._load_pypi_workflow(package_name, registry)
        except subprocess.CalledProcessError as e:
            logger.error(f"  ✗ Failed to install {package_name}: {e}")

        return False, None
