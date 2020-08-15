name := "CotacaoSpark"

version := "0.1"

scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0-beta"
)