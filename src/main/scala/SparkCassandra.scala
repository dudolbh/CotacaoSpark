import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

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

  val spark = SparkSession.builder
    .master(master)
    .appName(appName)
    .withExtensions(new CassandraSparkExtensions)
    .getOrCreate()

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val dfFromData1 = data.toDF()


}
