package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.types.StructType

class JsonSerde(schema: StructType) extends ValueSerde {
  override def serialize(df: DataFrame): DataFrame =
    df.select(to_json(struct("*")).as("value"))
  override def deserialize(df: DataFrame): DataFrame =
    df.select(from_json(col("value"), schema).as("value")).select("value.*")
}
