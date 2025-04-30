from writer import initialize_event_table, write_events

if __name__ == "__main__":
    initialize_event_table()

    test_data = [
        {
            "timestamp": 1142345000,
            "visitorid": 1132,
            "event": "view",
            "itemid": 10375,
            "transactionid": None
        },
        {
            "timestamp": 1142345022,
            "visitorid": 234,
            "event": "transaction",
            "itemid": 10548,
            "transactionid": 9001
        }
    ]

    write_events(test_data)