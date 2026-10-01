# Feast feature catalog

`features.py` defines offline visitor and item activity features against Parquet files produced by training. The API currently receives a visitor's recent item IDs directly and serves precomputed item neighbors, so it does not require an online feature database. To turn on low-latency online feature retrieval later, choose and configure a Feast online store before materializing these feature views.
