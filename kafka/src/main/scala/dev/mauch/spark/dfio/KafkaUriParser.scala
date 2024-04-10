package dev.mauch.spark.dfio

import org.apache.spark.sql.SparkSession
import UriHelpers._
import java.net.URI

class KafkaUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("kafka")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = {
    spark =>
        KafkaDataFrameSource(
          spark,
          broker = s"${uri.getHost}:${uri.getPort}",
          topic = uri.pathParts.head,
          serde = uri.queryParams.getOrElse("serde", "json"),
          additionalOptions = uri.queryParams.filterNot(_._1 == "serde")
        )
  }
}
