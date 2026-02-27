from volnux import Pipeline
from volnux.fields import InputDataField
import os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

class RagPipeline(Pipeline):
    """
    An AI-Powered RAG Ingestion Pipeline.

    Flow:
    1. DocReceived: Input document is received.
    2. (TextExtracted & MetadataExtracted): Parallel processing to extract text and metadata.
    3. EmbeddingsGenerated: Consumes both text and metadata to create vectors.
    4. Indexed: Stores everything in a Vector DB.
    """

    doc_path = InputDataField(
        data_type=str,
        required=True,
        default=os.path.join(_SCRIPT_DIR, "sample_document.txt")
    )

    class Meta:
        # Pointy-Lang DSL defining the flow
        # |-> : Sequential dependency (pipe pointer)
        # ||  : Parallel execution
        # Flow: DocReceived -> (TextExtracted || MetadataExtracted) -> TextChunked -> EmbeddingsGenerated -> Indexed
        pointy = "DocReceived |-> TextExtracted || MetadataExtracted |-> TextChunked |-> EmbeddingsGenerated |-> Indexed"
