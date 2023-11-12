# Databricks ML Demo End to End

An end to end DB Demo

# TODO

Add Monitoring Setup to the workflow code for `data_setup``

# Quick Notes

To refactor existing notebooks to work with DABS we needed to: 
- Move to parameterise through widgets

To speed up development we:
- Tagged jobs to an existing cluster

The DAB command `validate` is useful to see what variables can be used in parameterisation

The DAB resource definitions are not documented but can look at the API commands for each section:
- Experiments
- Jobs
- DLT

to see what is supported
