package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame

trait DataFrameSink {
  def write(df: DataFrame): Boolean
}
