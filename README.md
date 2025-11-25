<div style="display:flex; justify-content:center; align-items:center;">
    <img alt="volnux" src="img/volnux.svg" width="280" height="280" style="padding-bottom:0!important;margin-bottom:0!important;" />
</div>

# Introduction
**Simplify complex process automation with a flexible, high-performance framework.**

This library tackles the challenges of building reliable, scalable workflows by 
providing a clear separation between coordination and execution. It uses a declarative DSL, 
**Pointy-Lang**, to model your pipelines while managing the underlying complexity of concurrency, 
state, and task dependencies.

Build resilient automation that can handle anything from simple data processing to distributed, event-driven systems.

## Features
- **Pointy-Lang DSL**: Define task dependencies, conditional logic, and parallel execution using an intuitive, graph-based syntax.
- **Hybrid Concurrency**: Leverage asyncio for non-blocking I/O and multiprocessing for true parallel execution of CPU-bound tasks.
- **Distributed Processing**: Supports remote task execution and manages state across multiple processes with minimal overhead.
- **Extensible Architecture**: Easily integrate custom event logic, task executors, and signals to fit any use case.

## Installation
To install the framework, simply use pip:

```bash
pip install volnux
```