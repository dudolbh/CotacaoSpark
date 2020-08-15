import org.apache.spark.streaming
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.kafka010._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
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
  println("Schema pelo arquivo")
  println(schema)

  val schema2 = StructType(Array(
    StructField("Agencia_devolucao",StringType,true),
    StructField("Agencia_retirada",StringType,true),
    StructField("Canal",StringType,true),
    StructField("Data_cotacao",TimestampType,true),
    StructField("Data_devolucao",TimestampType,true),
    StructField("Data_retirada",TimestampType,true),
    StructField("Id_cotacao",StringType,true),
    StructField("ListaOferta",ArrayType(StructType(Array(
      StructField("Desconto",LongType,true),
      StructField("Grupo",StringType,true),
      StructField("Nome_promocao",StringType,true),
      StructField("Valor",LongType,true))),true),true),
    StructField("Logado",LongType,true)))

  println("Schema pelo Struc Type")
  println(schema2.printTreeString())

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

  val dflinhaJson = dflinha.select(col("key"), col("timestamp"),from_json(col("value"),schema2).as("value"))

  //val dflinhaJson = dfKafka.select($"key",from_json($"value"), $"timestamp")

  //val dflinhaJson = dfKafka.select(from_json(col("data")).select(from_json(col("data"),schema).as("data"))
  println("Schema dfKafka")
  println(dfKafka.printSchema())
  println("Schema linha (cast string)")
  println(dflinha.printSchema())
  println("Schema linha (cast from_json)")
  println(dflinhaJson.printSchema())

  val consoleOutput1 = dflinhaJson.select($"value.Id_cotacao", $"value.ListaOferta")
    .writeStream
    .outputMode("update")
    .format("console")
    .trigger(Trigger.Once())
    .start()

  spark.streams.awaitAnyTermination()
}
