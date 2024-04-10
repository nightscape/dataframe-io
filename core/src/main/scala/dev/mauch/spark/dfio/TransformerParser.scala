package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame

trait TransformerParser {
  def unapply(uri: java.net.URI): Option[DataFrame => DataFrame]
}
object TransformerParser extends TransformerParser {
  def unapply(uri: java.net.URI): Option[DataFrame => DataFrame] = {
    uri.getScheme match {
      case "identity" => Some(identity[DataFrame] _)
      case "sql" =>
        Some { df =>
          df.createOrReplaceTempView("input")
          df.sqlContext.sql(uri.getPath.substring(1))
        }
      case "sql-file" =>
        Some { df =>
          df.createOrReplaceTempView("input")
          df.sqlContext.sql(scala.io.Source.fromFile(uri.getPath.substring(1)).mkString)
        }
      case _ => None
    }
  }
}
