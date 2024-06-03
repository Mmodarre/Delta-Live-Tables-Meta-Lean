# Databricks notebook source
from dlt_meta_lean.dataflow_pipeline import *
layer = spark.conf.get("layer", None)
from dlt_meta_lean.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)