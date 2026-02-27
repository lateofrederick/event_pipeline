with open("sample_document.txt", "w") as f:
    for _ in range(1000):
        f.write("This is a test line for RAG pipeline evaluation. " * 10000 + "\n")