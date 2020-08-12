import org.apache.spark.streaming
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.kafka010._
import org.mortbay.util.ajax.JSON

object SparkStream extends App{
  val appName = "testeKafka";
  val master = "local[*]"
  val bootstrapServers = "localhost:9092"
  val topic = "al-nucleo-politicas-comerciais-cotacao-json"

  val spark = SparkSession.builder
    .master(master)
    .appName(appName)
    .getOrCreate()

  import spark.implicits._

  val dfKafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("fetchOffset.retryIntervalMs", 10000)
    .option("maxOffsetsPerTrigger", 10)
    .load()

  val dflinha = dfKafka.selectExpr("CAST(key AS STRING)", "to_json(CAST(value AS STRING))", "timestamp")
    .as[(String, String, Timestamp)]

  val dflinhaJson = dfKafka.select(from_json(col("data")).select(from_json(col("data"),schema).as("data"))

  println(dfKafka.printSchema())
  println(dflinha.printSchema())
  println(dflinhaJson.printSchema())

  val consoleOutput1 = dflinhaJson.writeStream
    .outputMode("update")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}
