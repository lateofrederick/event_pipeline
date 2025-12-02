import sys
import typing
import types
import logging
import importlib.util
from importlib import import_module
from pathlib import Path


logger = logging.getLogger(__name__)


def cached_import(module_path: str, class_name: str) -> typing.Type[typing.Any]:
    """
    Import a module and cache it in sys.modules
    Args:
        module_path (str): The module path to import
        class_name (str): The class name to retrieve from the module
    Returns:
        typing.Any: The imported class from the module
    """
    modules = sys.modules
    if module_path not in modules or (
        # Module is not fully initialized.
        getattr(modules[module_path], "__spec__", None) is not None
        and getattr(modules[module_path].__spec__, "_initializing", False) is True
    ):
        import_module(module_path)
    return getattr(modules[module_path], class_name)  # type: ignore


def import_string(dotted_path: str) -> typing.Type[typing.Any]:
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    Args:
        dotted_path (str): The dotted module path
    Returns:
        typing.Type: The imported attribute/class
    Raises:
        ImportError: If the module or attribute/class cannot be imported
        AttributeError: If the attribute/class does not exist in the module
    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    try:
        return cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError(
            'Module "%s" does not define a "%s" attribute/class'
            % (module_path, class_name)
        ) from err


def load_module_from_path(module_name: str, file_path: Path) -> types.ModuleType:
    """
    Dynamically load a Python module from a file path.

    Args:
        module_name: Name to assign to the loaded module
        file_path: Path to the Python file

    Returns:
        The loaded module object
    Raises:
        ImportError: If the module cannot be imported
        AttributeError: If the module cannot be imported
        Exception: If the module cannot be imported
    """
    if not file_path or not file_path.exists():
        raise ImportError(f"File {file_path} doesn't exist")

    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {file_path}")

    module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(module)
    except Exception:
        raise

    return module


def load_submodule(
    parent_module: types.ModuleType,
    parent_path: Path,
    submodule_name: str,
) -> typing.Optional[types.ModuleType]:
    """
    Dynamically load a submodule from a parent module.

    Supports loading submodules that exist as:
    - A Python file (submodule.py)
    - A package directory (submodule/__init__.py)
    - A package directory without __init__.py (namespace package)

    Args:
        parent_module: The parent module object
        parent_path: Path to the parent module file or directory
        submodule_name: Name of the submodule (with or without .py extension)

    Returns:
        Loaded submodule object, or None if not found or failed to load

    Example:
        >>> parent = load_module_from_path("workflow", Path("registry/workflow.py"))
        >>> pipeline = load_submodule(parent, Path("registry/workflow.py"), "pipeline")
        >>> event = load_submodule(parent, Path("registry/workflow.py"), "event.py")
    """
    parent_dir = parent_path.parent if parent_path.is_file() else parent_path

    base_name = submodule_name.removesuffix(".py")
    file_path = parent_dir / f"{base_name}.py"
    dir_path = parent_dir / base_name

    full_module_name = f"{parent_module.__name__}.{base_name}"

    if file_path.exists() and file_path.is_file():
        try:
            logger.debug(f"Loading submodule '{base_name}' from file: {file_path}")
            return load_module_from_path(full_module_name, file_path)
        except Exception as e:
            logger.warning(
                f"Failed to load submodule '{base_name}' from file {file_path}: {e}",
                exc_info=True,
            )
            return None

    if dir_path.exists() and dir_path.is_dir():
        init_path = dir_path / "__init__.py"

        if init_path.exists():
            try:
                logger.debug(f"Loading submodule '{base_name}' as package: {dir_path}")
                return load_module_from_path(full_module_name, init_path)
            except Exception as e:
                logger.warning(
                    f"Failed to load submodule '{base_name}' as package from {init_path}: {e}",
                    exc_info=True,
                )
                return None

        else:
            logger.warning(
                f"Submodule directory '{base_name}' found at {dir_path} but missing __init__.py. "
                f"Cannot load as package."
            )
            return None

    logger.debug(f"Submodule '{base_name}' not found in {parent_dir}")
    return None


def load_multiple_submodules(
    parent_module: types.ModuleType,
    parent_path: Path,
    submodule_names: typing.Iterable[str],
) -> typing.Dict[str, typing.Optional[types.ModuleType]]:
    """
    Load multiple submodules at once.

    Args:
        parent_module: The parent module object
        parent_path: Path to the parent module file or directory
        submodule_names: Iterable of submodule names to load

    Returns:
        Dictionary mapping submodule names to loaded modules (or None if failed)

    Example:
        >>> parent = load_module_from_path("workflow", Path("registry/workflow.py"))
        >>> modules = load_multiple_submodules(
        ...     parent,
        ...     Path("registry/workflow.py"),
        ...     ["pipeline", "event", "workflow"]
        ... )
        >>> pipeline = modules["pipeline"]
        >>> event = modules["event"]
    """
    result = {}
    for name in submodule_names:
        base_name = name.removesuffix(".py")
        result[base_name] = load_submodule(parent_module, parent_path, name)
    return result


def get_package_root_path(package_name: str) -> typing.Optional[Path]:
    """
    Finds and returns the absolute file system path to the root directory
    of an installed Python package.

    Args:
        package_name: The name of the installed package (e.g., 'requests', 'numpy').

    Returns:
        A Path object representing the package's root directory, or None if
        the package cannot be found.
    """
    try:
        spec = importlib.util.find_spec(package_name)

        if spec is None:
            logger.error(
                f"Error: Package '{package_name}' not found in Python environment."
            )
            return None

        package_origin = spec.origin

        if package_origin is None:
            logger.warning(
                f"Warning: Could not determine origin for package '{package_name}'."
            )
            return None

        origin_path = Path(package_origin)

        if origin_path.name.startswith("__init__."):
            root_path = origin_path.parent
        elif origin_path.is_dir():
            root_path = origin_path
        else:
            # For a single-file module (like 'os.py' for 'os'), the root is the file itself.
            root_path = origin_path

        return root_path.resolve()

    except Exception as e:
        logger.error(
            f"An unexpected error occurred while finding package '{package_name}': {e}"
        )
        return None
