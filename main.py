from ingestion.ingest import ingest
import time

if __name__ == "__main__":
    start_watch = time.perf_counter()
    ingest()
    print("Ingestion done")
    print(f"{time.perf_counter() - start_watch}s")