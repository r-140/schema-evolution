import threading

# Cutoff capture cannot race an in-flight raw batch with an older ingestion timestamp.
INGEST_LOCK = threading.Lock()
