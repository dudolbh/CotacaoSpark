import org.apache.spark.streaming
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.kafka010._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

// Spark SQL Cassandra imports
import com.datastax.spark.connector._

object SparkStream extends App{
  val appName = "testeKafka";
  val master = "local[*]"
  val bootstrapServers = "localhost:9092"
  val topic = "al-nucleo-politicas-comerciais-cotacao-json"

  val spark = SparkSession.builder
    .master(master)
    .appName(appName)
    .config("spark.sql.catalog.cotacao", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .config("spark.sql.catalog.cotacao.spark.cassandra.connection.host", "127.0.0.1:9042")
    .getOrCreate()

  import spark.implicits._

  val dfKafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("fetchOffset.retryIntervalMs", 10000)
    .option("maxOffsetsPerTrigger", 10)
    .load()
//    .option("startingOffsets", "earliest")

  val dflinha = dfKafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
    .as[(String, String, Timestamp)]

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


  val dflinhaJson = dflinha.select(col("key"), col("timestamp"),from_json(col("value"),schema2).as("value"))

  val schemaOferta = ArrayType(StructType(Array(
    StructField("desconto",LongType,true),
    StructField("grupo",StringType,true),
    StructField("nome_promocao",StringType,true),
    StructField("valor",LongType,true))))

  val dfCotacao = dflinhaJson.select($"value.Id_cotacao".alias("id_cotacao"),
                                    $"value.Agencia_retirada".alias("agencia_retirada"),
                                    $"value.Agencia_devolucao".alias("agencia_devolucao"),
                                    $"value.Data_retirada".alias("data_retirada"),
                                    $"value.Data_devolucao".alias("data_devolucao"),
                                    $"value.Canal".alias("canal"),
                                    $"value.Data_cotacao".alias("data_cotacao"),
                                    $"value.ListaOferta".alias("ofertas").cast(schemaOferta),
                                    $"value.Logado".alias("logado"))
  //val dflinhaJson = dfKafka.select($"key",from_json($"value"), $"timestamp")

  //val dflinhaJson = dfKafka.select(from_json(col("data")).select(from_json(col("data"),schema).as("data"))
  println("Schema original Kafka")
  println(dfKafka.printSchema())
  println("Schema linha (cast string)")
  println(dflinha.printSchema())
  println("Schema linha (cast from_json)")
  println(dflinhaJson.printSchema())
  println("Schema estruturado")
  println(dfCotacao.printSchema())

  /*val consoleOutput1 = dfKafka.writeStream
    .outputMode("update")
    .format("console")
    .trigger(Trigger.Once())
    .start()*/
  /*val consoleOutput2 = dflinhaJson.select($"value.Id_cotacao",
                                                $"value.Agencia_retirada",
                                                $"value.Agencia_retirada",
                                                $"value.Agencia_devolucao",
                                                $"value.Data_retirada",
                                                $"value.Data_devolucao",
                                                $"value.Canal",
                                                $"value.Data_cotacao",
                                                $"value.ListaOferta",
                                                $"value.Logado")
    .writeStream
    .outputMode("update")
    .format("console")
    .trigger(Trigger.Once())
    .start()*/
  val insertCasandra = dfCotacao.writeStream
    .outputMode("update")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.writeTo("cotacao.cotacao.cotacao").append()
    }
    .start()


   // .trigger(Trigger.Once())
  //Necessário pois o banco não aceita uma stream continua
  /*val dfCotacao2 = dfCotacao
  val outDfCassandra = dfCotacao2.writeStream
  .outputMode("update")
  .format("console")
  .trigger(Trigger.Once())
  .start()
*/
  spark.streams.awaitAnyTermination()
}

