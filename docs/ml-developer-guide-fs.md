# ML Developer Guide

[(back to main README)](../README.md)

## Table of contents
* [Initial setup](#initial-setup): adapting the provided example code to your ML problem 
* [Iterating on ML code](#iterating-on-ml-code): making and testing ML code changes on Databricks or your local machine.
* [Next steps](#next-steps)

## Initial setup
This project comes with [sample ML code](https://docs.databricks.com/machine-learning/feature-store/workflow-overview-and-notebook.html#example-notebook)
that illustrates the use of Feature Store to create a model that predicts NYC Yellow Taxi fares.

The subsequent sections explain how to adapt the sample code to your ML problem and quickly get
started iterating on feature engineering and model training code.

When you're ready  to productionize your ML project, ask your ops team to set up CI/CD and deploy
production jobs per the [MLOps setup guide](mlops-setup.md).

### Configure your ML pipeline

The sample ML code consists of the following:

* Feature computation modules under `db_ml_end_to_end/feature_engineering` folder. 
These sample module contains features logic that can be used to generate and populate tables in Feature Store.
In each module, there is `compute_features_fn` method that you need to implement. This should compute a features dataframe
(each column being a separate feature), given the input dataframe, timestamp column and time-ranges. 
The output dataframe will be persisted in a [time-series Feature Store table](https://docs.databricks.com/machine-learning/feature-store/time-series.html).
See the example modules' documentation for more information.
* Python unit tests for feature computation modules in `db_ml_end_to_end/tests/feature_engineering` folder.
* Feature engineering notebook, `db_ml_end_to_end/feature_engineering/notebooks/GenerateAndWriteFeatures.py`, that reads input dataframes, dynamically loads feature computation modules, executes their `compute_features_fn` method and writes the outputs to a Feature Store table (creating it if missing).
* Training notebook that [trains](https://docs.databricks.com/machine-learning/feature-store/train-models-with-feature-store.html ) a regression model by creating a training dataset using the Feature Store client.
* Model deployment and batch inference notebooks that deploy and use the trained model. 
* An automated integration test is provided (in `.github/workflows/db-ml-end-to-end-run-tests-fs.yml`) that executes a multi task run on Databricks involving the feature engineering and model training notebooks.

To adapt this sample code for your use case, implement your own feature module, specifying configs such as input Delta tables/dataset path(s) to use when developing
the feature engineering pipelines.
1. Implement your feature module, address TODOs in `db_ml_end_to_end/feature_engineering/features` and create unit test in `db_ml_end_to_end/tests/feature_engineering`
2. Update `db_ml_end_to_end/resources/feature-engineering-workflow-resource.yml`. Fill in notebook parameters for `write_feature_table_job`.
3. Update training data path in `db_ml_end_to_end/resources/model-workflow-resource.yml`.

We expect most of the development to take place in the `db_ml_end_to_end/feature_engineering` folder.

## Iterating on ML code

### Deploy ML code and resources to dev workspace using Bundles

Refer to [Local development and dev workspace](../db_ml_end_to_end/resources/README.md#local-development-and-dev-workspace)
to use databricks CLI bundles to deploy ML code together with ML resource configs to dev workspace.

### Develop on Databricks using Databricks Repos

#### Prerequisites
You'll need:
* Access to run commands on a cluster running Databricks Runtime ML version 11.0 or above in your dev Databricks workspace
* To set up [Databricks Repos](https://docs.databricks.com/repos/index.html): see instructions below

#### Configuring Databricks Repos
To use Repos, [set up git integration](https://docs.databricks.com/repos/repos-setup.html) in your dev workspace.

If the current project has already been pushed to a hosted Git repo, follow the
[UI workflow](https://docs.databricks.com/repos/git-operations-with-repos.html#add-a-repo-connected-to-a-remote-repo)
to clone it into your dev workspace and iterate. 

Otherwise, e.g. if iterating on ML code for a new project, follow the steps below:
* Follow the [UI workflow](https://docs.databricks.com/repos/git-operations-with-repos.html#add-a-repo-connected-to-a-remote-repo)
  for creating a repo, but uncheck the "Create repo by cloning a Git repository" checkbox.
* Install the `dbx` CLI via `pip install --upgrade dbx`
* Run `databricks configure --profile db-ml-end-to-end-dev --token --host <your-dev-workspace-url>`, passing the URL of your dev workspace.
  This should prompt you to enter an API token
* [Create a personal access token](https://docs.databricks.com/dev-tools/auth.html#personal-access-tokens-for-users)
  in your dev workspace and paste it into the prompt from the previous step
* From within the root directory of the current project, use the [dbx sync](https://dbx.readthedocs.io/en/latest/guides/python/devloop/mixed/#using-dbx-sync-repo-for-local-to-repo-synchronization) tool to copy code files from your local machine into the Repo by running
  `dbx sync repo --profile db-ml-end-to-end-dev --source . --dest-repo your-repo-name`, where `your-repo-name` should be the last segment of the full repo name (`/Repos/username/your-repo-name`)

#### Running code on Databricks
You can iterate on ML code by running the provided `db_ml_end_to_end/feature_engineering/notebooks/GenerateAndWriteFeatures.py` notebook on Databricks using
[Repos](https://docs.databricks.com/repos/index.html). This notebook drives execution of
the feature transforms code defined under ``features``. You can use multiple browser tabs to edit
logic in `features` and run the feature engineering pipeline in the `GenerateAndWriteFeatures.py` notebook.

### Develop locally

You can iterate on the feature transform modules locally in your favorite IDE before running them on Databricks.  

#### Prerequisites
* Python 3.8+
* Install feature engineering code and test dependencies via `pip install -I -r db_ml_end_to_end/requirements.txt -r test-requirements.txt` from project root directory.
* The features transform code uses PySpark and brings up a local Spark instance for testing, so [Java (version 8 and later) is required](https://spark.apache.org/docs/latest/#downloading). 
#### Run unit tests
You can run unit tests for your ML code via `pytest tests`.

## Next Steps
If you're iterating on ML code for an existing, already-deployed ML project, follow [Submitting a Pull Request](ml-pull-request.md)
to submit your code for testing and production deployment.

Otherwise, if exploring a new ML problem and satisfied with the results (e.g. you were able to train
a model with reasonable performance on your dataset), you may be ready to productionize your ML pipeline.
To do this, ask your ops team to follow the [MLOps Setup Guide](mlops-setup.md) to set up CI/CD and deploy
production training/inference pipelines.

[(back to main README)](../README.md)
