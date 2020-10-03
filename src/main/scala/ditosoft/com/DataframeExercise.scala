package ditosoft.com

import java.lang

import org.apache.spark.sql.functions.{col, when, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class DataframeExercise (implicit sparkSession:SparkSession) {

  val personasDF: DataFrame = readTxtFileWithSchema()
    .withColumn("genero", when(col("sexo")==="F", "Femenino").otherwise("Masculino"))
    .withColumn("nacionalidad", lit("colombiana"))

  personasDF.show(numRows = 3, truncate = false)
  println("El schema del *Dataframe* es: " + personasDF.schema)

  repartitionExercise()
  writeDataframeAsParquet(personasDF)
  turnDataframeToDataset(personasDF)
  aggregationFunctions(personasDF)


  /**************************************** Functions ***************************************************/
  def repartitionExercise(): Unit ={
    val personasDF: DataFrame = readTxtFileWithSchema()

    val myDf: Dataset[lang.Long] = sparkSession.range(0,10000) // repartition= 5, porque .master(["local"])
    val myDf1:DataFrame = myDf.repartition(numPartitions = 8).toDF() //repartition= 5
    println("rdd partitions: " + myDf.rdd.partitions.size) //5
    println("rdd1 partitions: " + myDf1.rdd.partitions.size) // 8

    /* EJERCICIO 1: Particionando Dataframe con un número máximo (8) de archivos por partición(partitionKey)
    Cada partición de disco tendrá hasta 8 archivos. Si en femenino tengo 3 pesonas se crean 3 archivos
    cada uno con una persona. Esto no es lo ideal.*/
    personasDF
      .repartition(8, col("sexo"), rand)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("sexo")
      .csv("resources/filesResults/1_dataframe_exercise_repartition_1")

    /* EJERCICIO 2: Particionando Dataframe con el max numero de registros por archivo
     Si en femenino tengo 11 personas con la siguiente configuracion me crea dentro de
     la carpeta sexo=F 3 archivos, uno dos con 5 registros y uno con 1 registro.
    * */
    personasDF
      .repartition(1, col("sexo"), rand)
      .write
      .mode(SaveMode.Overwrite)
      .option("maxRecordsPerFile", 5)
      .partitionBy("sexo")
      .csv("resources/filesResults/1_dataframe_exercise_repartition_2")
  }

  def turnDataframeToDataset(myDf:DataFrame): Unit ={
    val myDataset = myDf.toDF()
    println("El schema del *Dataset* es: " + myDataset.schema)
  }

  def writeDataframeAsParquet(myDf: DataFrame): Unit ={
    myDf.write
      .partitionBy("sexo")
      .format(PARQUET)
      .mode(SaveMode.Overwrite)  //append, ignore, error, errorifexists
      .save("resources/filesResults/personasParquet")
  }

  def readTxtFileWithSchema():DataFrame = {
    val customSchema1 = StructType(Array (StructField("nombre", StringType),
      StructField("apellido", StringType), StructField("edad", IntegerType),
      StructField("sexo", StringType), StructField("salario", IntegerType),
      StructField("carro", StringType), StructField("apartamento", StringType))
    )

    val dataframePersona = sparkSession.read
      .format("csv")
      .schema(customSchema1)
      .option("header","true")
      .option("delimiter", "|")
      .load("resources/filesToReadInExercises/Personas.csv")
    dataframePersona
  }

  def aggregationFunctions(myDf:DataFrame)={
    val salario = "salario"
    val df = myDf
      .filter(col("edad") > 15)
      .groupBy("sexo")
      .agg(
        sum(salario).alias("suma_salarios"),
        min(salario).alias("minimo_salario"),
        max(salario).alias("maximo_salario")
      ).show()
  }

}
