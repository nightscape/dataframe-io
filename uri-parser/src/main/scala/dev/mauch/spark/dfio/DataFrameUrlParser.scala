package dev.mauch.spark.dfio

import org.apache.spark.sql.SparkSession

import java.net.URI
import scala.jdk.CollectionConverters._

import UriHelpers._

class DataFrameUrlParser(_spark: SparkSession) {
  def unapply(s: String): Option[DataFrameSource with DataFrameSink] = {
    val uri = new java.net.URI(s).parseServerAuthority()
    if (DataFrameUrlParser.isDefinedAt(uri)) {
      Some(DataFrameUrlParser(uri)(_spark))
    } else {
      None
    }
  }

}
object DataFrameUrlParser extends DataFrameUriParser {
   private def registry = java.util.ServiceLoader.load[DataFrameUriParser](classOf[DataFrameUriParser])
   private val uriParsers = registry.iterator().asScala.toList
   def schemes: Seq[String] = uriParsers.flatMap(_.schemes)

  def apply(uri: URI): SparkSession => DataFrameSource with DataFrameSink = {
    println(s"""
               |scheme:    ${uri.getScheme}
               |user:      ${uri.getUserInfo}
               |authority: ${uri.getAuthority}
               |fragment:  ${uri.getFragment}
               |host:      ${uri.getHost}
               |port:      ${uri.getPort}
               |path:      ${uri.getPath}
               |query:     ${uri.getQuery}
               |""".stripMargin)

    val providers = registry.iterator().asScala.toList
    providers.collectFirst { case p if p.isDefinedAt(uri) => p(uri) }.get
  }
}
