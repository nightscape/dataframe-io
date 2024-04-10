package dev.mauch.spark.dfio

import org.apache.spark.sql.SparkSession

trait DataFrameUriParser extends PartialFunction[java.net.URI, SparkSession => DataFrameSource with DataFrameSink] {
  def schemes: Seq[String]
  def isDefinedAt(uri: java.net.URI): Boolean = schemes.contains(uri.getScheme)
}

