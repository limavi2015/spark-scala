package ditosoft.com

import org.apache.spark.sql.SparkSession

object Main extends App {

  implicit val sparkSession: SparkSession = SparkSession
    .builder
    .appName("my-app")
    .master(LOCAL)
    .enableHiveSupport()
    .config("spark.sql.shuffle.parallelism", "8") // # de todos los núcleos en todos los nodos de un clúster, en local se establece en el número de núcleos en su sistema.
    .config("spark.sql.shuffle.partitions", "100") //Es usado cuando llamas a operaciones de reproducción aleatoria como reduceByKey, groupByKey, join
    .getOrCreate()

  val sc = sparkSession.sparkContext
  sc.setLogLevel("WARN")

  val exercise = "DatasetExercise"

  exercise match {
    case "RddExercise" => new RddExercise()
    case "DataframeExercise" => new DataframeExercise()
    case "UdfExercise" => new UdfExercise()
    case "DatasetExercise" => new DatasetExercise()
  }




}
