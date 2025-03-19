package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant

import UriHelpers._

case class AvroFileDataFrameSource(spark: SparkSession, path: String, options: Map[String, String] = Map.empty)
    extends DataFrameSource
    with DataFrameSink {

  val finalOptions = Map("inferSchema" -> "true", "header" -> "true") ++ options
  override def read(): DataFrame = {
    spark.read
      .format("avro")
      .options(finalOptions)
      .load(path)
  }

  override def write(df: DataFrame): Boolean = {
    df.select(DataFrameUtils.flattenSchema(df.schema): _*)
      .write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .options(finalOptions)
      .save(path)
    true
  }
}

class AvroUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("avro")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    new AvroFileDataFrameSource(spark, uri.getPath, options = uri.queryParams)
  }
}
