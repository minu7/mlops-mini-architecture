# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import Entity, FeatureService, FeatureView, Field, PushSource, RequestSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Bool, Int32, String, Float32

driver = Entity(name="driver", join_keys=["id"])



drivers_source = PostgreSQLSource(
        name="drivers_source",
        query="SELECT * FROM drivers",
        timestamp_field="created_at",
)


drivers = FeatureView(
    name="drivers",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="id", dtype=Int32),
        Field(name="label", dtype=Bool),
        Field(name="amt", dtype=Float32),
        Field(name="kids_driv", dtype=Int32),
        Field(name="age", dtype=Int32),
        Field(name="home_kids", dtype=Int32),
        Field(name="yoj", dtype=Int32),
        Field(name="income", dtype=Float32),
        Field(name="parent1", dtype=Bool),
        Field(name="home_val", dtype=Float32),
        Field(name="m_status", dtype=Bool),
        Field(name="sex", dtype=String),
        Field(name="education_level", dtype=Int32),
        Field(name="job", dtype=String),
        Field(name="trav_time", dtype=Int32),
        Field(name="commercial_car_use", dtype=Bool),
        Field(name="blue_book", dtype=Float32),
        Field(name="tif", dtype=Int32),
        Field(name="car_type", dtype=String),
        Field(name="red_car", dtype=Bool),
        Field(name="old_claim", dtype=Float32),
        Field(name="clm_freq", dtype=Int32),
        Field(name="revoced", dtype=Bool),
        Field(name="mvr_pts", dtype=Int32),
        Field(name="car_age", dtype=Int32),
        Field(name="urban_city", dtype=Bool),
    ],
    source=drivers_source,
)

