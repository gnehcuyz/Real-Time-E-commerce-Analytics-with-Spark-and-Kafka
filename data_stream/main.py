import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_stream.streaming_pipeline import run_streaming_pipeline
from data_sink.writer import initialize_event_table

if __name__ == "__main__":
    initialize_event_table()
    run_streaming_pipeline()
