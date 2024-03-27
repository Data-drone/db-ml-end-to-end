# Databricks ML Demo End to End

An end to end DB Demo

# Deploying the assets

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
