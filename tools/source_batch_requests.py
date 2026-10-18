"""Enable independent daily generation requests in the existing batch ledger."""

from sqlalchemy import text


def ensure_generation_request_schema(connection) -> None:
    """Migrate metadata atomically under the generator's advisory lock."""
    exists = connection.execute(text("""
        SELECT EXISTS (SELECT 1 FROM information_schema.columns
        WHERE table_schema='pipeline' AND table_name='batches'
          AND column_name='generation_request_id')
    """)).scalar_one()
    if exists:
        return
    connection.execute(text("ALTER TABLE pipeline.batches ADD COLUMN generation_request_id text"))
    connection.execute(text("""
        ALTER TABLE pipeline.batches DROP CONSTRAINT IF EXISTS
        batches_dataset_id_business_date_batch_kind_key
    """))
    connection.execute(text("""
        CREATE UNIQUE INDEX uq_pipeline_batch_date_kind
        ON pipeline.batches(dataset_id,business_date,batch_kind)
        WHERE batch_kind <> 'manual'
    """))
    connection.execute(text("""
        CREATE UNIQUE INDEX uq_pipeline_generation_request
        ON pipeline.batches(dataset_id,generation_request_id)
        WHERE generation_request_id IS NOT NULL
    """))
