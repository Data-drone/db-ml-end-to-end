# Databricks notebook source
# MAGIC %md
# MAGIC # Deploying and using models
# MAGIC We will explore how to deploy models get make them useful

# COMMAND ----------

# DBTITLE 1,Configurations
catalog = dbutils.widgets.text("catalog", "brian_ml") #'brian_ml'
schema = dbutils.widgets.text("schema", "taxi_example") #'taxi_example'
model_name = 'taxi_example_fare_time_series_packaged'

workload_type = 'CPU'
workload_sizing = 'Small'
endpoint_name = 'brian_ml_taxi_example_fare_time_series_packaged'

function_name =  dbutils.widgets.text("function_name", "taxi_fare_prediction")  #'taxi_fare_prediction'

# COMMAND ----------

%run ./endpoint_utils

# COMMAND ----------

# MAGIC %md # Deploy Registered Model to Endpoint

latest_version = get_latest_model_version(f'{catalog}.{schema}.{model_name}')

# COMMAND ----------

serving_client = EndpointApiClient()

# NOTE this can take 10 to 15 mins
serving_client.create_endpoint_if_not_exists(endpoint_name, 
                                            model_name=f"{catalog}.{db}.{model_name}", 
                                            model_version = latest_version, 
                                            workload_size=workload_sizing,
                                            workload_type=workload_type
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

endpoint_url = f"{serving_client.base_url}/realtime-inference/{endpoint_name}/invocations"
print(f"Sending requests to {endpoint_url}")
starting_time = timeit.default_timer()
inferences = requests.post(endpoint_url, json=dataset, headers=serving_client.headers).json()
print(f"Embedding inference, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms {inferences}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION ${catalog}.${schema}.${function_name}(
# MAGIC     trip_distance DOUBLE, pickup_zip INT, dropoff_zip INT, mean_fare_window_1h_pickup_zip FLOAT,
# MAGIC     count_trips_window_1h_pickup_zip INT, count_trips_window_30m_dropoff_zip INT, dropoff_is_weekend INT
# MAGIC )
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC def predict_from_endpoint(trip_distance, pickup_zip, dropoff_zip, mean_fare_window_1h_pickup_zip,
# MAGIC                           count_trips_window_1h_pickup_zip, count_trips_window_30m_dropoff_zip,
# MAGIC                           dropoff_is_weekend):
# MAGIC     
# MAGIC     endpoint_url = 'TODO'
# MAGIC     headers = 'TODO' # serving_client.headers
# MAGIC 
# MAGIC     dataset = {"dataframe_records": [
# MAGIC         {
# MAGIC             "trip_distance": trip_distance,
# MAGIC             "pickup_zip": pickup_zip,
# MAGIC             "dropoff_zip": dropoff_zip,
# MAGIC             "mean_fare_window_1h_pickup_zip": mean_fare_window_1h_pickup_zip,
# MAGIC             "count_trips_window_1h_pickup_zip": count_trips_window_1h_pickup_zip,
# MAGIC             "count_trips_window_30m_dropoff_zip": count_trips_window_30m_dropoff_zip,
# MAGIC             "dropoff_is_weekend": dropoff_is_weekend
# MAGIC         }
# MAGIC     ]}
# MAGIC 
# MAGIC     import requests
# MAGIC     result = requests.post(endpoint_url, json=dataset, headers=headers).json()
# MAGIC 
# MAGIC     return result
# MAGIC  
# MAGIC return predict_from_endpoint(trip_distance, pickup_zip, dropoff_zip, mean_fare_window_1h_pickup_zip,
# MAGIC                           count_trips_window_1h_pickup_zip, count_trips_window_30m_dropoff_zip,
# MAGIC                           dropoff_is_weekend)
# MAGIC $$

# COMMAND ----------