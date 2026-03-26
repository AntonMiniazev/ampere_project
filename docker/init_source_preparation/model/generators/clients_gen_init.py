import random
from datetime import date, datetime, timedelta

import polars as pl
from faker import Faker

fake = Faker()


def generate_clients(
    n: int = 100,
    store_id_range: tuple[int, int] = (1, 5),
    project_start_date: str | None = None,
) -> pl.DataFrame:
    """Generate the initial client population for the source schema.

    These rows are the starting point for the whole synthetic pipeline: later
    daily generation marks some of them as churned, adds new registrations, and
    attaches orders to the active client base. The output shape mirrors the
    `source.clients` table so bootstrap code can upload it directly.
    """
    if project_start_date is None:
        project_start_date = date.today().isoformat()
    start_date = datetime.strptime(project_start_date, "%Y-%m-%d").date()

    # Generate realistic-looking identities and spread registrations slightly
    # backward in time so the source table does not look like it was created in
    # one single minute.
    fullnames = [f"{fake.first_name()} {fake.last_name()}" for _ in range(n)]
    preferred_store_ids = [random.randint(*store_id_range) for _ in range(n)]
    days_ago = [random.randint(0, 14) for _ in range(n)]
    registration_dates = [start_date - timedelta(days=delta) for delta in days_ago]
    churned = [False] * n

    return pl.DataFrame(
        {
            "fullname": fullnames,
            "preferred_store_id": preferred_store_ids,
            "registration_date": registration_dates,
            "churned": churned,
        }
    )
