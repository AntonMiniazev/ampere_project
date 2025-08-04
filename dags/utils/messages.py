# utils/messages.py

def build_client_message(row):
    """
    Takes a row from SQL result (tuple) and returns a formatted message with client_id.
    Example input: ('ORD001', 123, 'store_1', ...) → "This is client number #123"
    """
    if not row or len(row) < 2:
        return "⚠️ Invalid data received."

    client_id = row[1]  # Assumes client_id is the second column in the row
    return f"This is client number #{client_id}"
