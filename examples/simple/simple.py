from event_pipeline.pipeline import Pipeline, BatchPipeline
from event_pipeline.fields import InputDataField


class Simple(Pipeline):
    name = InputDataField(data_type=list, batch_size=5)


class SimpleBatch(BatchPipeline):
    pipeline_template = Simple
