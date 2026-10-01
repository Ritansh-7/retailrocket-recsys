"""Feast feature definitions for offline historical retrieval from parquet artifacts."""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int64

visitor = Entity(name="visitor", join_keys=["visitor_id"])
item = Entity(name="item", join_keys=["item_id"])

visitor_features_source = FileSource(
    name="visitor_training_features_source",
    path="../artifacts/visitor_features.parquet",
    timestamp_field="event_timestamp",
)
item_features_source = FileSource(
    name="item_training_features_source",
    path="../artifacts/item_features.parquet",
    timestamp_field="event_timestamp",
)

visitor_activity = FeatureView(
    name="visitor_activity",
    entities=[visitor],
    ttl=timedelta(days=3650),
    schema=[Field(name="event_count", dtype=Int64), Field(name="unique_items", dtype=Int64)],
    source=visitor_features_source,
    online=False,
)

item_activity = FeatureView(
    name="item_activity",
    entities=[item],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="interaction_count", dtype=Int64),
        Field(name="unique_visitors", dtype=Int64),
        Field(name="popularity_score", dtype=Float64),
    ],
    source=item_features_source,
    online=False,
)
