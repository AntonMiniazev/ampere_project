import random
from importlib.resources import as_file, files

import polars as pl


def generate_assortment() -> pl.DataFrame:
    dictionaries = files("init_source_preparation.dictionaries")

    with (
        as_file(dictionaries / "products.csv") as product_path,
        as_file(dictionaries / "stores.csv") as store_path,
    ):
        df_products = pl.read_csv(product_path)
        df_store = pl.read_csv(store_path)

    category_ids = df_products.get_column("category_id").unique().to_list()
    store_ids = df_store.get_column("id").to_list()

    assortment_frames = []

    for store_id in store_ids:
        for category_id in category_ids:
            category_products = df_products.filter(
                pl.col("category_id") == category_id
            )
            category_len = category_products.height
            if category_len == 0:
                continue

            num_ones = random.randint(1, 3)
            num_ones = min(num_ones, category_len)
            remove_indices = set(random.sample(range(category_len), k=num_ones))
            keep_indices = [
                idx for idx in range(category_len) if idx not in remove_indices
            ]

            if not keep_indices:
                continue

            selected = (
                category_products.with_row_count("row_index")
                .filter(pl.col("row_index").is_in(keep_indices))
                .select(
                    pl.col("id").alias("product_id"),
                    pl.lit(store_id).alias("store_id"),
                )
            )
            assortment_frames.append(selected)

    if not assortment_frames:
        return pl.DataFrame({"product_id": [], "store_id": []})

    return pl.concat(assortment_frames, how="vertical")
