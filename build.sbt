name := "ejercicios_spark"

version := "0.1"

logLevel := Level.Warn

scalaVersion := "2.11.6"

resolvers ++= Seq(
  Resolver.mavenLocal,
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client"   % "2.7.0",
  "org.apache.spark"  % "spark-core_2.11" % sparkVersion,
  "org.apache.spark"  % "spark-sql_2.11"  % sparkVersion,
  "org.apache.spark"  %% "spark-hive"     % "2.4.3"
)