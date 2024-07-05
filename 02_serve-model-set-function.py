# Databricks notebook source
# MAGIC %md
# MAGIC # Deploying and using models
# MAGIC We will explore how to deploy models get make them useful

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# DBTITLE 1,Configurations
dbutils.widgets.text("catalog", "brian_ml") #'brian_ml'
dbutils.widgets.text("schema", "taxi_example") #'taxi_example'

catalog = dbutils.widgets.get("catalog") #'brian_ml'
schema = dbutils.widgets.get("schema") #'taxi_example'
model_name = 'taxi_example_fare_time_series_packaged'

workload_type = 'CPU'
workload_sizing = 'Small'
endpoint_name = 'brian_ml_taxi_example_fare_time_series_packaged'

dbutils.widgets.text("function_name", "taxi_fare_prediction")  #'taxi_fare_prediction'
function_name = dbutils.widgets.get("function_name")

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md # Create Online Store
# MAGIC We need to setup the table for online store to do serving

# COMMAND ----------

from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec
from databricks.feature_store.client import FeatureStoreClient
 
fs = FeatureStoreClient()
 
# Specify the online store.
# Note: these commands use the predefined secret prefix. If you used a different secret scope or prefix, edit these commands before running them.

online_store_spec = AmazonDynamoDBSpec(
  region="us-west-2",
  write_secret_prefix="one-env-dynamodb-fs-read/field-eng",
  read_secret_prefix="one-env-dynamodb-fs-write/field-eng",
  table_name = "feature_store_brian_ml_trip_pickup_time_series_features",
)
 
# Push the feature table to online store.
fs.publish_table(f"{catalog}.{schema}.trip_pickup_time_series_features", 
                 online_store_spec,
                 mode='merge')

# COMMAND ----------

online_store_spec = AmazonDynamoDBSpec(
  region="us-west-2",
  write_secret_prefix="one-env-dynamodb-fs-read/field-eng",
  read_secret_prefix="one-env-dynamodb-fs-write/field-eng",
  table_name = "feature_store_brian_ml_trip_dropoff_time_series_features",
)
 
# Push the feature table to online store.
fs.publish_table(f"{catalog}.{schema}.trip_dropoff_time_series_features", 
                 online_store_spec,
                 mode='merge')

# COMMAND ----------

# MAGIC %md # Deploy Registered Model to Endpoint

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput, AutoCaptureConfigInput

w.serving_endpoints.create_and_wait(
    name = endpoint_name,
    config = EndpointCoreConfigInput(
        auto_capture_config = AutoCaptureConfigInput(
            catalog_name=catalog,
            schema_name=schema,
            enabled=True,
        ),
        served_entities=[
            ServedEntityInput(
                entity_name = f'{catalog}.{schema}.{model_name}',
                entity_version = '1',
                workload_size = 'Small',
                scale_to_zero_enabled = True
            )
        ]
    )
)

# COMMAND ----------

# MAGIC %md ## Test out serving endpoint

# COMMAND ----------

dataset = {"dataframe_records": [
        {
            "trip_distance": 5.35,
            "pickup_zip": 10003,
            "dropoff_zip": 11238,
            "mean_fare_window_1h_pickup_zip": 12.5,
            "count_trips_window_1h_pickup_zip": 1,
            "count_trips_window_30m_dropoff_zip": 1,
            "dropoff_is_weekend": 1
        }
    ]}

import timeit
import requests

endpoint_url = f"https://{workspace_url}/realtime-inference/{endpoint_name}/invocations"
print(f"Sending requests to {endpoint_url}")
starting_time = timeit.default_timer()

# this is for notebook use only - create one for prop
NOTEBOOK_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
model_serving_headers = {'Authorization': f'Bearer {NOTEBOOK_TOKEN}',
                         'Content-Type': 'application/json'}
inferences = requests.post(endpoint_url, json=dataset, headers=model_serving_headers)
print(f"Embedding inference, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms {inferences}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Discussion
# MAGIC We now have the endpoint setup and can query it using the ai_query function
# MAGIC this needs to be done from a SQL Warehouse but we can connect one

# COMMAND ----------

%sql

SELECT ai_query(
  'brian_ml_taxi_example_fare_time_series_packaged',
   named_struct(
    'trip_distance', CAST(5.35 AS DOUBLE),
    'pickup_zip', 10003,
    'dropoff_zip', 11238,
    'mean_fare_window_1h_pickup_zip', CAST(12.5 AS DOUBLE),
    'count_trips_window_1h_pickup_zip', 1,
    'count_trips_window_30m_dropoff_zip', 1,
    'dropoff_is_weekend', 1
   ),
  'DOUBLE'
);

# COMMAND ----------

