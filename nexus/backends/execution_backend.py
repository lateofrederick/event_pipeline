from abc import ABC, abstractmethod


class ExecutionBackend(ABC):
    @abstractmethod
    def streamInto(self, pipeline):
        """Submit a pipeline for execution, returns future-like object"""
        pass

    @abstractmethod
    def GetResultFutures(self, pipeline_batch):
        """Get futures for a batch of pipelines"""
        pass
    
    @abstractmethod
    def resolve(self, futures):
        """Resolve futures to actual results"""
        pass
    
    @abstractmethod
    def shutdown(self):
        """Clean up resources"""
        pass
