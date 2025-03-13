package dev.mauch.spark.dfio

import zio._
import zio.test._
import zio.test.Assertion._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde._
import zio.stream._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import com.dimafeng.testcontainers.KafkaContainer
import zio.kafka.consumer.Consumer.OffsetRetrieval
import zio.kafka.consumer.Consumer.AutoOffsetStrategy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import java.nio.file.{Files, Paths}
import java.io.File

object ETLTest extends ZIOSpecDefault {
  val sparkLayer: ZLayer[Scope, Throwable, SparkSession] = ZLayer.scoped(ZIO.acquireRelease {
    ZIO.succeed {
      SparkSession
        .builder()
        .appName("ETLTest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    }
  }(spark => ZIO.succeed(spark.close())))

  case class Person(id: Long, name: String, age: Int)
  val exampleData: List[Person] = List(
    Person(1, "Alice", 30),
    Person(2, "Bob", 25),
    Person(3, "Charlie", 35),
    Person(4, "Dave", 40),
    Person(5, "Eve", 22)
  )

  private val testDeltaPath = {
    val tempDir = Files.createTempDirectory("dataframe-io-test").toFile()
    tempDir.deleteOnExit()
    tempDir.getAbsolutePath()
  }

  private def producerSettings(bootstrapServers: String): ProducerSettings =
    ProducerSettings(List(bootstrapServers))
      .withClientId("test-producer")
      .withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000")

  private def consumerSettings(bootstrapServers: String): ConsumerSettings =
    ConsumerSettings(List(bootstrapServers))
      .withGroupId("test-group")
      .withClientId("test-consumer")
      .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withCloseTimeout(5.seconds)

  def spec = suite("ETL Test")(
    test("should run ETL from Kafka to Delta") {
      for {
        spark <- ZIO.service[SparkSession]
        randomNumber <- Random.nextIntBounded(1000)
        topic <- ZIO.succeed(s"test-topic-$randomNumber")
        kafka <- ZIO.service[KafkaContainer]
        bootstrapServers = kafka.bootstrapServers
        producer <- Producer.make(producerSettings(bootstrapServers))
        _ <- ZIO.foreach(exampleData) { person =>
          val json = s"""{"id": ${person.id}, "name": "${person.name}", "age": ${person.age}}"""
          producer.produce(new ProducerRecord(topic, "key1", json), Serde.string, Serde.string)
        }
        _ <- ZIO.attempt {
          val args = Array(
            "--master",
            "local[*]",
            "--source",
            s"kafka://${bootstrapServers.replaceFirst("PLAINTEXT://", "")}/$topic?serde=json",
            "--sink",
            s"console://foo",
            "--sink",
            s"delta://$testDeltaPath"
          )
          println(args.mkString(" "))
          ETL.main(args)
        }
        result <- ZIO.attempt {
          val deltaDF = spark.read.format("delta").load(testDeltaPath)
          val rows = deltaDF.collect()
          rows
        }

      } yield {
        assert(result.length)(equalTo(exampleData.length)) &&
        assert(result.map(_.getAs[Long]("id")).toSet)(equalTo(exampleData.map(_.id).toSet)) &&
        assert(result.map(_.getAs[String]("name")).toSet)(equalTo(exampleData.map(_.name).toSet))
      }
    } @@ TestAspect.timeout(60.seconds),
    test("should run streaming ETL from Kafka to Delta") {
      for {
        spark <- ZIO.service[SparkSession]
        randomNumber <- Random.nextIntBounded(1000)
        topic <- ZIO.succeed(s"test-topic-$randomNumber")
        kafka <- ZIO.service[KafkaContainer]
        bootstrapServers = kafka.bootstrapServers
        producer <- Producer.make(producerSettings(bootstrapServers))
        dataStream = ZStream.fromIterable(exampleData)
        sentDataStream = dataStream.mapZIO(person => {
          val json = s"""{"id": ${person.id}, "name": "${person.name}", "age": ${person.age}}"""
          producer.produce(new ProducerRecord(topic, "key1", json), Serde.string, Serde.string).as(person)
        })
        _ <- sentDataStream.runDrain
        _ <- ZIO.attempt {
          val schema = org.apache.spark.sql.Encoders.product[Person].schema
          val schemaURL = java.net.URLEncoder.encode(schema.json, "UTF-8")
          val args = Array(
            "--master",
            "local[*]",
            "--source",
            s"kafka-stream://${bootstrapServers.replaceFirst("PLAINTEXT://", "")}/$topic?serde=json:$schemaURL&startingOffsets=earliest",
            "--sink",
            s"delta://$testDeltaPath?checkpointLocation=$testDeltaPath/checkpoint"
          )
          println(args.mkString(" "))
          ETL.main(args)
        }
        result <- ZIO.attempt {
          var rows: Array[org.apache.spark.sql.Row] = Array.empty
          val maxAttempts = 30
          var attempt = 0
          while (attempt < maxAttempts) {
            try {
              val deltaDF = spark.read.format("delta").load(testDeltaPath)
              if (deltaDF.count() >= exampleData.length) {
                rows = deltaDF.collect()
                attempt = maxAttempts
              } else {
                attempt += 1
                Thread.sleep(1000)
              }
            } catch {
              case e: Exception =>
                attempt += 1
                Thread.sleep(1000)
            }
          }
          rows
        }
      } yield {
        assert(result.length)(equalTo(exampleData.length)) &&
        assert(result.map(_.getAs[Long]("id")).toSet)(equalTo(exampleData.map(_.id).toSet)) &&
        assert(result.map(_.getAs[String]("name")).toSet)(equalTo(exampleData.map(_.name).toSet))
      }
    } @@ TestAspect.timeout(60.seconds)
  )
    .provideSomeLayerShared[Scope with KafkaContainer](sparkLayer)
    .provideSomeLayerShared(DockerLayer.kafkaTestContainerLayer)
}
