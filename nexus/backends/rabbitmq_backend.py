import pika
from .execution_backend import ExecutionBackend


class RabbitMQBackend(ExecutionBackend):
    def __init__(self, amqp_url, queue_name):
        self.connection = pika.BlockingConnection(
            pika.URLParameters(amqp_url)
        )
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name, durable=True)

    def streamInto(self, pipeline):
        """Submit a pipeline for execution, returns future-like object"""
        pass

    def GetResultFutures(self, pipeline_batch):
        """Get futures for a batch of pipelines"""
        pass
    
    def resolve(self, futures):
        """Resolve futures to actual results"""
        pass
    
    def shutdown(self):
        """Clean up resources"""
        pass
