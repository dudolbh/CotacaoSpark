import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

// Spark SQL Cassandra imports
import com.datastax.spark.connector._


object SparkCassandra extends App{
  // read the configuration file

  /*val sparkConf = new SparkConf()
    .setAppName("GravacaoCassandra")
    .set(s"spark.sql.catalog.teste", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .set(s"spark.sql.catalog.cass100.spark.cassandra.connection.host", "127.0.0.100")*/

  val appName = "testeCassandra";
  val master = "local[*]"

  //iniciando a sessão
  val spark = SparkSession.builder
    .master(master)
    .appName(appName)
    //.withExtensions(new CassandraSparkExtensions)
    .config("spark.sql.catalog.teste", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .config("spark.sql.catalog.teste.spark.cassandra.connection.host", "127.0.0.1:9042")
    .getOrCreate()

  //Criando um dataset
  import spark.implicits._
  val colunas = Seq("palavra","quantidade")
  val data = Seq(("Ruby", "20000"), ("C#", "100000"), ("Angular", "3000"))
  var dfDados = spark.createDataFrame(data).toDF(colunas:_*)

  println(dfDados.show())
  //Gravação no Cassandra
  dfDados.writeTo("teste.teste.palavras").append()

  //Criação simples
  //val dfFromData1 = data.toDF()
  //Com colunas
  //println(dfFromData1.show())
  /*dfFromData1.select(col("_1").as("palavra"),col("_2").as("quantidade"))
    .writeTo("teste.teste.palavras")
    .append()*/

  /*dfFromData1.select(col("_1").as("palavra"),col("_2").as("quantidade"))
    .write
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "teste")
    .option("table","palavras")
    .mode("APPEND")
    .save()*/

}
