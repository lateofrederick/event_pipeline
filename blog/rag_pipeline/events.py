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
