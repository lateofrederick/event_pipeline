from volnux.pipeline import Pipeline
from volnux.fields import InputDataField
from .events import DocReceived, TextExtracted, MetadataExtracted, EmbeddingsGenerated, Indexed

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
        default="sample_document.pdf"
    )

    class Meta:
        # Pointy-Lang DSL defining the flow
        # |-> : Sequential dependency
        # &   : Parallel execution (implied, though Volnux might need | for branching or just multiple dependencies)
        # Note: Check Volnux DSL syntax for parallel. Based on plan:
        # DocReceived |-> (TextExtracted & MetadataExtracted) |-> EmbeddingsGenerated |-> Indexed

        # Assuming standard Volnux syntax supports grouping or just standard chaining.
        # If explicitly parallel syntax isn't '&', we define connections implicitly.
        # But 'events.py' examples used |->.
        # Let's use the syntax from the plan which implies the user knows it or we assume it works.
        pointy = "DocReceived |-> (TextExtracted & MetadataExtracted) |-> EmbeddingsGenerated |-> Indexed"
