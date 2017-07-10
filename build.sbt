name := "confluent-spark-avro"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.1.1" % "provided"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.2.2"

libraryDependencies += "com.databricks" %% "spark-avro" % "3.2.0"

libraryDependencies += "org.scalaz" % "scalaz-core_2.11" % "7.3.0-M14"

// Include provided dependencies when "sbt run" is called.
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
