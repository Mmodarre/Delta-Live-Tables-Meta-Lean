# Databricks notebook source
from dlt_meta_lean.dataflow_pipeline import DataflowPipeline
layer = spark.conf.get("layer", None) #pylint: disable=undefined-variable # type: ignore
DataflowPipeline.invoke_dlt_pipeline(spark, layer) #pylint: disable=undefined-variable # type: ignore
