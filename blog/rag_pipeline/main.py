import logging
import os
import shutil
from .pipeline import RagPipeline

# Configure logging to see the events in action
logging.basicConfig(level=logging.INFO)

def main():
    # Setup: Ensure we have a sample PDF
    # We'll use the one found in the repo root if available, otherwise strict fail (as it's a demo)
    # Copied for the blog folder context
    sample_pdf = "sample_document.pdf"
    repo_pdf = "../../pointylang_vs_airflow_prefect_beam.pdf"

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
