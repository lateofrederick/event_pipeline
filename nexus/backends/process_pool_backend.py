from concurrent.futures import ProcessPoolExecutor
from .execution_backend import ExecutionBackend
from ..pipeline import Pipeline


def execute_pipeline(pipeline: Pipeline):
    return pipeline.start(force_rerun=True)


class ProcessPoolBackend(ExecutionBackend):
    def __init__(self, max_workers=None):
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
    
    def streamInto(self, pipeline: Pipeline):
        """Submit a pipeline for execution, returns future-like object"""
        return self.executor.submit(execute_pipeline, pipeline)

    def GetResultFutures(self, pipeline_batch):
        """Get futures for a batch of pipelines"""
        return [self.streamInto(pipeline) for pipeline in pipeline_batch]
    
    def resolve(self, futures):
        """Resolve futures to actual results"""
        return [future.result() for future in futures]
    
    def shutdown(self):
        """Clean up resources"""
        self.executor.shutdown(wait=True)

