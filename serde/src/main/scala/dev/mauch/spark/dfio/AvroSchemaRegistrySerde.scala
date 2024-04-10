package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct}
import za.co.absa.abris.avro.functions.{from_avro, to_avro}
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}

/** Retrieves the schema from the schema registry and uses it to serialize and deserialize the data.
  * @param topicName
  *   The name of the topic for which to lookup the schema
  * @param schemaRegistryConfig
  *   The configuration for the schema registry (see [[za.co.absa.abris.config.AbrisConfig]] for details)
  */
class AvroSchemaRegistrySerde(topicName: String, schemaRegistryConfig: Map[String, String]) extends ValueSerde {

  private val fromAvroAbrisConfig: FromAvroConfig =
    AbrisConfig.fromConfluentAvro.downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topicName)
      .usingSchemaRegistry(schemaRegistryConfig)

  private val toAvroAbrisConfig: ToAvroConfig =
    AbrisConfig.toConfluentAvro.downloadSchemaByLatestVersion
      .andTopicNameStrategy(topicName)
      .usingSchemaRegistry(schemaRegistryConfig)
  override def serialize(df: DataFrame): DataFrame =
    df.select(to_avro(struct("*"), toAvroAbrisConfig).as("value"))
  override def deserialize(df: DataFrame): DataFrame =
    df.select(from_avro(col("value"), fromAvroAbrisConfig).as("value"))
      .select("value.*")
}

object AvroSchemaRegistrySerde {
  def unapply(sparkConfig: Map[String, String]): Option[AvroSchemaRegistrySerde] = {
    val topicNameOpt = sparkConfig.get("schema.topic.name")
    val schemaRegistryConfig = sparkConfig.filterKeys(key => key.startsWith("schema.registry."))
    // Abris expects some keys without the "schema.registry." prefix (e.g. basic.auth...),
    // so we provide both versions.
    val schemaRegistryConfigPlusTruncatedKeys = schemaRegistryConfig ++ schemaRegistryConfig.map { case (key, value) =>
      key.replace("schema.registry.", "") -> value
    }
    for {
      // Only create the serde if the schema registry url is set
      _ <- schemaRegistryConfig.get(AbrisConfig.SCHEMA_REGISTRY_URL)
      topicName <- topicNameOpt
    } yield new AvroSchemaRegistrySerde(topicName, schemaRegistryConfigPlusTruncatedKeys.toMap)
  }
}
