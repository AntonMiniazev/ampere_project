# Get products from Products.csv
# Take random products from each category
# Assign result to store from Store.csv
# cols: [product_id, store_id]

import pandas as pd
import numpy as np
import importlib.resources


def generate_assortment():
    # Use context manager for product_path
    with (
        importlib.resources.path("dictionaries", "products.csv") as product_path,
        importlib.resources.path("dictionaries", "stores.csv") as store_path,
    ):
        # Read Product and Store tables
        df_products = pd.read_csv(product_path)
        df_store = pd.read_csv(store_path)

        num_of_categories = len(df_products["category_id"].unique())

        df_assortment = pd.DataFrame()

        for idx in df_store["id"]:
            for cat_id in range(1, num_of_categories + 1):
                category_len = len(
                    df_products[df_products["category_id"] == cat_id])
                mask = np.zeros(category_len, dtype=int)
                num_ones = np.random.randint(1, 4)
                ones_indices = np.random.choice(
                    category_len, size=num_ones, replace=False
                )
                mask[ones_indices] = 1

                df_assortment = pd.concat(
                    [
                        df_assortment,
                        df_products[df_products["category_id"] == cat_id]
                        .loc[mask == 0]
                        .assign(store_id=idx)[["id", "store_id"]],
                    ],
                    ignore_index=True,
                )
    return df_assortment.rename(columns={"id": "product_id"})
