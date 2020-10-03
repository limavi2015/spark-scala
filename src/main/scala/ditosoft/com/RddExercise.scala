package ditosoft.com

import scala.reflect.io.Directory
import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class RddExercise() (implicit sparkSession:SparkSession) {
  import sparkSession.implicits._

  val developersRDD= createRddFromASet()

  turnRddToDataframe(developersRDD)
  saveRddInATextFile(developersRDD)
  repartitionExercice()

  /**************************************** Functions ***************************************************/
  def repartitionExercice()={
    val rdd: RDD[Int] = sparkSession.sparkContext.parallelize(Range(0,20)) // repartition= 5, porque .master(["local"])
    val rdd1 = sparkSession.sparkContext.parallelize(Range(0,25), numSlices = 6) //repartition = 6
    val rdd2 = rdd.coalesce(numPartitions =4) //repartition = 4, lo toma bien porque el # de particiones es menor
    val rdd3 = rdd.coalesce(numPartitions =9) //repartition= 5, porque .master(["local"]) y el 9 > 5
    val rddFromFile = sparkSession.sparkContext.textFile(FILE_PERSONAS_TO_TEST,minPartitions = 10) //repartition=10

    println("rdd partitions: " + rdd.partitions.size) //5
    println("rdd1 partitions: " + rdd1.partitions.size) //6
    println("rdd2 partitions: " + rdd2.partitions.size) //4
    println("rdd3 partitions: " + rdd3.partitions.size) //5
    println("rddFromFile partitions: " + rddFromFile.partitions.size) //10
  }

  def saveRddInATextFile(myRdd:RDD[(String, String, String)]): Unit ={
    deleteFolder()
    myRdd.saveAsTextFile("resources/filesResults/desarrolladores.txt")
    println(myRdd.collect())
  }

  def createRddFromASet(): RDD[(String, String, String)] ={
    val data = Seq(
      ("Java", "20000", "colombia"), ("Python", "100000", "colombia"), ("Scala", "3000","colombia"),
      ("Java", "20000", "chile"), ("Python", "100000", "chile"), ("Scala", "3000","chile"),
      ("Java", "20000", "peru"), ("Python", "100000", "peru"), ("Scala", "3000","peru")
    )
    val rdd: RDD[(String, String, String)] = sparkSession.sparkContext.parallelize(data)  //"local[5]"
    printRdd(rdd)
    rdd
  }

  def turnRddToDataframe(myRdd: RDD[(String, String, String)]): Unit ={
    val myDataframe = myRdd.toDF()
    println("El schema del *Dataset* es: " + myDataframe.schema)
  }

  def printRdd(rdd: org.apache.spark.rdd.RDD[_]): Unit = rdd.foreach(println)

  def deleteFolder(pathURl:String="resources/filesResults")={
    val directory = new Directory(new File(pathURl))
    directory.deleteRecursively()
  }
}

