import org.apache.spark.streaming
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.kafka010._
import org.apache.spark.sql.types.{StructField, StructType}
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

  val schema = spark.read.json("C:\\Users\\eduar\\OneDrive\\PosEngDados\\Proj Aplicado\\Arquivos\\Sprint 4\\cotacao.json").schema
  println(schema)

  val dfKafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("fetchOffset.retryIntervalMs", 10000)
    .option("maxOffsetsPerTrigger", 10)
    .load()

  val dflinha = dfKafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
    .as[(String, String, Timestamp)]

  val dflinhaJson = dflinha.select(col("key"), col("timestamp"),from_json(col("value"),schema).as("value"))

  //val dflinhaJson = dfKafka.select($"key",from_json($"value"), $"timestamp")

  //val dflinhaJson = dfKafka.select(from_json(col("data")).select(from_json(col("data"),schema).as("data"))

  println(dfKafka.printSchema())
  println(dflinha.printSchema())
  println(dflinhaJson.printSchema())

  val consoleOutput1 = dflinhaJson.writeStream
    .outputMode("update")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}
