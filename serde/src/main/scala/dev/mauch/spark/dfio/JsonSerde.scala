package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, struct, schema_of_json, to_json}
import org.apache.spark.sql.types.StructType

class JsonSerde(schema: Option[StructType] = None) extends ValueSerde {
  override def serialize(df: DataFrame): DataFrame =
    df.select(to_json(struct("*")).as("value"))
  override def deserialize(df: DataFrame): DataFrame = {
    val strs = df.select(col("value").cast("string").as("value"))
    schema.map(s => strs.select(from_json(col("value"), s).as("value")).select("value.*")).getOrElse {
      val spark = df.sparkSession
      import spark.implicits._
      spark.read.json(strs.select("value").as[String])
    }
  }
}
