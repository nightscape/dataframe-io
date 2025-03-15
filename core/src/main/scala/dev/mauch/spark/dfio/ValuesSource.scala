package dev.mauch.spark.dfio

import java.time.Instant
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import UriHelpers._
import java.net.URI

case class ValuesSource(spark: SparkSession, header: Seq[String], values: Seq[Seq[String]])
    extends DataFrameSource
    with DataFrameSink {
  def valuesToDF(f: Seq[String] => Row): DataFrame = {
    import spark.implicits._
    spark.createDataFrame(
      java.util.Arrays.asList(values.map(f): _*),
      StructType(header.map(StructField(_, StringType)))
    )
  }
  override def read(): DataFrame = {
    import spark.implicits._
    header match {
      case Seq(a) => valuesToDF { case Seq(a) => Row(a) }
      case Seq(a, b) =>
        valuesToDF { case Seq(a, b) => Row(a, b) }
      case Seq(a, b, c) =>
        valuesToDF { case Seq(a, b, c) => Row(a, b, c) }
      case Seq(a, b, c, d) =>
        valuesToDF { case Seq(a, b, c, d) => Row(a, b, c, d) }
      case Seq(a, b, c, d, e) =>
        valuesToDF { case Seq(a, b, c, d, e) => Row(a, b, c, d, e) }
      case Seq(a, b, c, d, e, f) =>
        valuesToDF { case Seq(a, b, c, d, e, f) => Row(a, b, c, d, e, f) }
    }
  }
  override def write(df: DataFrame): Boolean = {
    df.show(10000, false)
    true
  }

}

class ValuesUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("values")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    new ValuesSource(
      spark,
      header = uri.queryParams.getOrElse("header", "").split(",").toSeq,
      values = uri.queryParams
        .getOrElse("values", "")
        .split(";")
        .map(_.split(",").toSeq)
        .toSeq
    )
  }
}
