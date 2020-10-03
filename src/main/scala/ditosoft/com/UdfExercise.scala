package ditosoft.com
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.SparkSession

class UdfExercise (implicit sparkSession:SparkSession) {

  val squared = udf((s: Long) => s * s)

  sparkSession.range(1, 8).select(squared(col("id")) as "id_squared").show


}
