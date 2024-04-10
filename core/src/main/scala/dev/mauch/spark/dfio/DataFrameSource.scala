package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame

import java.time.Instant

trait DataFrameSource {
  def read(): DataFrame
}
