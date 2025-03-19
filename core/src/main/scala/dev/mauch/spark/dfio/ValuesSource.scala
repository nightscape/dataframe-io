package dev.mauch.spark.dfio

import java.time.Instant
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import UriHelpers._
import java.net.URI

case class ValuesSource(spark: SparkSession, header: Seq[(String, DataType)], values: Seq[Seq[String]])
    extends DataFrameSource
    with DataFrameSink {
  def valuesToDF(f: Seq[String] => Row): DataFrame = {
    import spark.implicits._
    spark.createDataFrame(
      java.util.Arrays.asList(values.map(f): _*),
      StructType(header.map { case (name, dt) => StructField(name, dt) })
    )
  }
  override def read(): DataFrame = {
    def convertValue(s: String, dt: DataType): Any = dt match {
      case IntegerType => s.toInt
      case DoubleType => s.toDouble
      case LongType => s.toLong
      case _ => s
    }
    valuesToDF { row =>
      Row.fromSeq(header.zip(row).map { case ((_, dt), value) => convertValue(value, dt) })
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
    val headerStr = uri.queryParams.getOrElse("header", "")
    val parsedHeader: Seq[(String, DataType)] = headerStr
      .split(",")
      .filter(_.nonEmpty)
      .map { field =>
        val parts = field.split(":")
        if (parts.length == 2)
          (
            parts(0),
            parts(1).trim.toLowerCase match {
              case "int" => IntegerType
              case "double" => DoubleType
              case "long" => LongType
              case _ => StringType
            }
          )
        else (field, StringType)
      }
      .toSeq

    new ValuesSource(
      spark,
      header = parsedHeader,
      values = uri.queryParams
        .getOrElse("values", "")
        .split(";")
        .map(_.split(",").toSeq)
        .toSeq
    )
  }
}
