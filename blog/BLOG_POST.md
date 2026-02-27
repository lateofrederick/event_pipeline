# Building an AI-Powered RAG Pipeline with Volnux

_From the heart of a developer who's seen too many broken pipelines._

---

## Introduction

We've all been there. You start with a simple script. "I just need to process this PDF," you say. It’s easy. You write a few functions: `extract_text()`, `get_metadata()`, `save_to_db()`. Look at you go!

But then, reality hits. Or rather, _latency_ hits.

"Can we run the metadata extraction in parallel with the text parsing?" your manager asks. "And hey, if the embedding API times out, can we retry just that part without re-reading the file?"

Suddenly, your simple script is a mess of `asyncio.gather`, `try/except` blocks nested three levels deep, and a global state variable that you're pretty sure is thread-safe (spoiler: it's not).

Building reliable, scalable pipelines for AI—especially for RAG (Retrieval-Augmented Generation)—is harder than it looks. You're not just coding logic; you're orchestrating chaos.

That's where **Volnux** comes in.

## The Challenge

In modern AI engineering, we deal with heavy, potentially slow operations.

- **Text Extraction**: CPU intensive.
- **Embedding Generation**: GPU intensive or high-latency API calls.
- **Vector Indexing**: Network bound.

If you run these sequentially, your users wait. If you try to hand-roll concurrency, you introduce bugs. You need a way to say, "Do _this_, then do _those two things at the same time_, and when they're both done, do _that_."

And you want to say it without rewriting your entire codebase.

## The Solution: Volnux

Volnux is a library designed to separate the _what_ from the _when_. You define your logic in isolated, testable "Events" (think of them as tasks), and then you define the flow using **Pointy-Lang**, a beautiful, graphical DSL that looks just like your whiteboard drawing.

Let’s build something real.

## Deep Dive: Building a RAG Ingestion Pipeline

Let's imagine we are building a Knowledge Base for our startup. We drop a PDF into a bucket, and we want it searchable in our chat bot.

Here is our plan:

1.  **Ingestion**: Receive the document path.
2.  **Parallel Processing**:
    - **Extract Metadata**: Get author/dates (IO bound).
3.  **Chunking**: Split the text into optimal pieces for our LLM context window (CPU intensive).
4.  **Embedding**: Generate vectors for _all chunks_ in a high-performance batch.
5.  **Indexing**: Push to `chromadb`.

### The Events

We use `pypdf`, `langchain`, and `chromadb` here. Notice how **Volnux** lets us mix IO-bound (database/fs) and CPU-bound (chunking/embedding) tasks effortlessly.

```python
# blog/rag_pipeline/events.py
import logging
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
from pypdf import PdfReader
from langchain_text_splitters import RecursiveCharacterTextSplitter
import os
import uuid

from volnux import EventBase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global model loading (optimization)
# Ensure we are using a model that runs locally and efficiently
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# Global ChromaDB client (persistent)
chroma_client = chromadb.Client(Settings(persist_directory="./chroma_db", is_persistent=True))
collection = chroma_client.get_or_create_collection(name="knowledge_base")

class DocReceived(EventBase):
    """
    Event triggered when a new document is received.
    For this example, we assume the input is a file path to a PDF.
    """
    def process(self, doc_path=None, **kwargs):
        # Volnux passes InputDataField values as a single dict positional arg
        if isinstance(doc_path, dict):
            doc_path = doc_path.get("doc_path", "")

        if not os.path.exists(doc_path):
            raise FileNotFoundError(f"Document not found: {doc_path}")

        logger.info(f"Received document: {doc_path}")
        return True, doc_path

class TextExtracted(EventBase):
    """
    Extracts raw text from the PDF document using pypdf.
    """
    def process(self, *args, **kwargs):
        doc_path = self.previous_result[0].content
        logger.info(f"Extracting text from {doc_path}...")

        try:
            reader = PdfReader(doc_path)
            extracted_text = ""
            for page in reader.pages:
                extracted_text += page.extract_text() + "\n"

            logger.info(f"Text extraction complete. Length: {len(extracted_text)} chars")
            return True, extracted_text
        except Exception as e:
            logger.error(f"Failed to extract text: {e}")
            return False, str(e)

class MetadataExtracted(EventBase):
    """
    Extracts metadata from the PDF document using pypdf.
    """
    def process(self, *args, **kwargs):
        doc_path = self.previous_result[0].content
        logger.info(f"Extracting metadata from {doc_path}...")

        try:
            reader = PdfReader(doc_path)
            # pypdf metadata keys usually have a slash like '/Author'
            meta = reader.metadata
            clean_metadata = {
                "author": meta.get("/Author", "Unknown"),
                "creation_date": meta.get("/CreationDate", "Unknown"),
                "source": doc_path
            }
            logger.info(f"Metadata extracted: {clean_metadata}")
            return True, clean_metadata
        except Exception as e:
            logger.error(f"Failed to extract metadata: {e}")
            return False, str(e)

class TextChunked(EventBase):
    def process(self, *args, **kwargs):
        text_content = ""
        metadata = {}

        # Inspect all inputs from the parallel branching
        for result in self.previous_result:
            if isinstance(result.content, str):
                text_content = result.content
            elif isinstance(result.content, dict):
                metadata = result.content

        logger.info(f"Chunking text length {len(text_content)}...")

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,
            chunk_overlap=50
        )
        chunks = text_splitter.split_text(text_content)

        logger.info(f"Generated {len(chunks)} chunks.")
        return True, {"chunks": chunks, "metadata": metadata}

class EmbeddingsGenerated(EventBase):
    def process(self, *args, **kwargs):
        data = self.previous_result[0].content
        chunks = data["chunks"]
        metadata = data["metadata"]

        logger.info(f"Generating embeddings for {len(chunks)} chunks in batch...")

        # Batch embedding generation
        embeddings = embedding_model.encode(chunks)

        logger.info(f"Generated embeddings matrix: {embeddings.shape}")
        return True, {"embeddings": embeddings, "chunks": chunks, "metadata": metadata}

class Indexed(EventBase):
    """
    Indexes the embeddings and metadata into ChromaDB.
    """
    def process(self, *args, **kwargs):
        data = self.previous_result[0].content
        embeddings = data["embeddings"]
        chunks = data["chunks"]
        metadata = data["metadata"]

        logger.info(f"Indexing {len(chunks)} chunks into ChromaDB...")

        try:
            ids = [str(uuid.uuid4()) for _ in chunks]
            # Chroma requires a non-empty metadata dict for EACH document
            if not metadata:
                metadata = {"source": "unknown"}
            metadatas = [metadata for _ in chunks]

            collection.add(
                documents=chunks,
                embeddings=embeddings.tolist(),
                metadatas=metadatas,
                ids=ids
            )

            logger.info(f"Indexing complete. Inserted {len(ids)} records.")
            return True, f"Indexed {len(ids)} chunks"
        except Exception as e:
            logger.error(f"Failed to index: {e}")
            return False, str(e)
```

### The Pipeline

The architecture is explicitly defined. Volnux handles the complex state passing (like getting metadata safely through the chunking step).

```python
# blog/rag_pipeline/pipeline.py
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
```

### Visualizing the Flow

One of the most powerful features of Volnux is its ability to automatically generate visual representations of your pipelines. By simply calling `pipeline.draw_graphviz_image()` in your script (or `draw_ascii_graph()` for a purely terminal-based output), Volnux parses your `pointy` definition and builds a graph of your exact execution topology.

Honestly, when you're dealing with messy, unpredictable AI processes, having a visual map is a lifesaver. You don't have to hold the entire architecture in your head or squint at nested concurrent calls trying to figure out what happens when. You just look at the picture. For our RAG pipeline above, the generated graph below tells the whole story: you can instantly see the text and metadata extraction splitting off to run at the same time, and then waiting for each other right before the chunking step kicks off:

![Rag Pipeline Graph](https://drive.google.com/uc?export=view&id=11LhR-Kb91STDRXKnoTd42skZajfQzBbm)

### Running the Pipeline

Volnux pipelines are plain Python objects. You just instantiate and start them.

```python
# blog/rag_pipeline/main.py
import logging
import os
import shutil
from .pipeline import RagPipeline

# Configure logging to see the events in action
logging.basicConfig(level=logging.INFO)

# Resolve paths relative to this script's directory, not the working directory
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    # Setup: Ensure we have a sample PDF
    # We'll use the one found in the repo root if available, otherwise strict fail (as it's a demo)
    # Copied for the blog folder context
    sample_pdf = os.path.join(_SCRIPT_DIR, "sample_document.txt")
    repo_pdf = os.path.join(_SCRIPT_DIR, "../../pointylang_vs_airflow_prefect_beam.pdf")

    if not os.path.exists(sample_pdf):
        if os.path.exists(repo_pdf):
            print(f"Copying {repo_pdf} to {sample_pdf}...")
            shutil.copy(repo_pdf, sample_pdf)
        else:
            print(f"Warning: {sample_pdf} not found. Please provide a PDF file.")
            # We won't crash here, we let the pipeline handle the error naturally!

    print("-" * 50)
    print("Starting AI RAG Ingestion Pipeline...")
    print("-" * 50)

    # Initialize the pipeline
    # The default 'doc_path' in pipeline.py is "sample_document.pdf", so we don't need to pass args
    # unless we want to override it.
    pipeline = RagPipeline()

    # Run it!
    # Volnux handles the execution graph, parallelizing where possible.
    pipeline.start()

    print("-" * 50)
    print("Pipeline Execution Complete.")
    print("-" * 50)

if __name__ == "__main__":
    main()
```

When you run this, you'll see the logs showing `TextExtracted` and `MetadataExtracted` running simultaneously, followed by the heavy `TextChunked` job, and finally the batch `EmbeddingsGenerated`.

### Why This Matters

1.  **Extreme Performance**: By batching the embedding generation (the most expensive step), we get massive speedups compared to looping. Volnux natively supports passing these variable-sized batches between steps.
2.  **Modular Heavy Lifting**: The `TextChunked` event is a pure CPU task. In a distributed Volnux setup, you could route just this task to a high-CPU node, while `EmbeddingsGenerated` goes to a GPU node.
3.  **Clarity**: You see exactly where the data splits and merges.

## Alternatives & Tradeoffs

"But why not just use X?"

It's a fair question. Volnux isn't the only game in town, but it occupies a specific sweet spot.

### The Alternatives

- **Airflow / Prefect**:
  - **Good for**: Scheduled nightly ETL jobs.
  - **Shortfall**: They are often overkill for real-time, event-driven apps. Spinning up a DAG for single-document ingestion can feel heavy. Plus, you often end up with "spaghetti DAGs" where logic is mixed with config.
- **Celery**:
  - **Good for**: Raw distributed task execution.
  - **Shortfall**: It's a fantastic execution engine (in fact, Volnux can use it!), but it lacks a high-level DSL. Defining complex flows like "Run A, then B&C in parallel, then D with the results of B&C" requires burying logic inside your tasks or using complex `chord`/`chain` primitives that are hard to visualize.
- **LangGraph / Native Chains**:
  - **Good for**: Pure LLM prototyping.
  - **Shortfall**: Great for the "AI" part, but often struggle when you need to integrate heavy infrastructure, retries, legacy systems, or complex error handling outside the happy path.

### Where Volnux Shines

- **Complex or Changing Topologies**: If you find yourself drawing boxes and arrows on a whiteboard to explain your code, Volnux's `pointy` DSL maps 1:1 to that drawing.
- **Infrastructure/Logic Separation**: You want your AI engineers writing `Events` (pure Python logic) and your Platform engineers managing the `Pipeline` (concurrency, retries, resources).
- **Observability**: You need to know exactly _which_ step failed and why, without grepping through monolithic logs.

### When NOT to use Volnux

- **Simple "Cron" Scripts**: If you just need to run a script every hour to cleanup a database, Volnux is overkill. Use `cron` or a simple lambda.
- **Ultra-Low Latency**: If your pipeline needs to run in microseconds (e.g., high-frequency trading), the framework overhead might be too much. Volnux is fast, but it prioritizes safety and structure over raw nanosecond speed.

## Conclusion

Coding doesn't have to be a battle against complexity. Tools like Volnux exist to handle the heavy lifting of orchestration so you can focus on the business logic—the parts that actually generate value.

Give it a try. Your future self (and your users) will thank you.

happy coding!
