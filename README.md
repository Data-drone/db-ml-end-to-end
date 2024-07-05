# Databricks ML Demo End to End

An end to end DB Demo

# Deploying the assets

The yamls may need config first.
The workspace is hardcoded in inside `databricks.yml`
the yaml files under `resources` have a hard-coded cluster id for execution rather than using a jobs cluster for faster startup time.

Those settings will need to be adjusted for your environment.

Once they are configured then we can first off the bundles (assuming you have already setup bundles and Databricks CLI on your local dev machine)

*Tips* 
- Make sure that your cli is up to date
- make sure that your configurations are clean


```{bash}
# check auth and token setup

databricks bundle validate

databricks bundle deploy -t dev
databricks bundle run -t dev data_setup
databricks bundle run -t dev data_ingest
databricks bundle run -t dev build_features
```

# TODO

Add Monitoring Setup to the workflow code for `data_setup``

# Quick Notes

To refactor existing notebooks to work with DABS we needed to: 
- Move to parameterise through widgets

To speed up development we:
- Tagged jobs to an existing cluster
- we should parameterise this so that it is easier to configure

The DAB command `validate` is useful to see what variables can be used in parameterisation

The DAB resource definitions are not documented but can look at the API commands for each section:
- Experiments
- Jobs
- DLT

to see what is supported
