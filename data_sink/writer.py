import psycopg2
from psycopg2.extras import execute_values
from config.database_config import POSTGRES_CONFIG


def initialize_event_table():
    """Creates the events table if it does not exist."""
    create_query = """
    CREATE TABLE IF NOT EXISTS events (
        timestamp BIGINT,
        visitor_id BIGINT,
        event_type TEXT,
        item_id BIGINT,
        transaction_id BIGINT
    );
    """
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(create_query)

def safe_int(val):
    try:
        str_val = str(val).strip().strip('"').strip("'")
        if str_val.lower() == "nan":
            return None
        return int(float(str_val))
    except (ValueError, TypeError):
        return None

def write_events(events: list[dict]):
    """Inserts a list of event dictionaries into the events table."""
    insert_query = """
    INSERT INTO events (timestamp, visitor_id, event_type, item_id, transaction_id)
    VALUES %s
    """

    values = [
        (
            safe_int(e["timestamp"]),
            safe_int(e["visitorid"]),
            str(e["event"]),
            safe_int(e["itemid"]),
            safe_int(e["transactionid"])
        )
        for e in events
    ]

    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
            conn.commit()
