# utils/messages.py

def build_client_message(row):
    """
    Takes a row from SQL result (dict or tuple) and returns a formatted message with client_id.
    Example input:
      - dict: {'client_id': 123, ...}
      - tuple: ('ORD001', 123, 'store_1', ...)  # deprecated format
    Returns:
      "This is client number #123"
    """
    if not row:
        return "⚠️ Invalid data received."

    try:
        if isinstance(row, dict):
            client_id = row["client_id"]
        else:  # fallback to tuple
            client_id = row[1]
        return f"This is client number #{client_id}"
    except (IndexError, KeyError, TypeError):
        return "⚠️ Failed to extract client_id."
