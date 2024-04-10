package dev.mauch.spark.dfio

object UriHelpers {
  implicit class RichUri(uri: java.net.URI) {
    def queryParams: Map[String, String] =
      Option(uri.getQuery)
        .map(
          _.split("&")
            .map(_.split("="))
            .filter(_.nonEmpty)
            .map { case Array(k, v) => k -> v }
            .toMap
        )
        .getOrElse(Map.empty)

    def pathParts = uri.getPath.split("/").filter(_.nonEmpty)
    def schemeSourceSink: (String, Option[String], Option[String]) = {
      val scheme = uri.getScheme
      scheme.split("\\+") match {
        case Array(source, sink, scheme) => (scheme, Some(source).filter(_.nonEmpty), Some(sink).filter(_.nonEmpty))
        case Array(source, scheme) => (scheme, Some(source).filter(_.nonEmpty), None)
        case Array(scheme) => (scheme, None, None)
      }
    }
    def schemeAndName: (String, Option[String]) = {
      val scheme = uri.getScheme
      scheme.split("\\+") match {
        case Array(name, scheme) => (scheme, Some(name))
        case Array(scheme) => (scheme, None)
      }
    }
  }
}
