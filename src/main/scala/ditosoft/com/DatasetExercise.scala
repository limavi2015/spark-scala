package ditosoft.com

import org.apache.spark.sql.SparkSession

case class Tomador(name: String, age: Long)

class DatasetExercise (implicit sparkSession:SparkSession) {
  import sparkSession.implicits._

  val caseClassDS = Seq(
    Tomador("Andy", 32),
    Tomador("Mary", 25),
    Tomador("Adri", 22),
    Tomador("Martin", 30),
    Tomador("Luis", 32),
    Tomador("Juli", 32)).toDS()
  caseClassDS.show()

}
