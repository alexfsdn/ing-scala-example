package ingestion.base.fake

import ingestion.base.dados.ISpark
import ingestion.base.fake.schema.ExampleBaseInterna
import ingestion.base.services.SparkSessionServices
import ingestion.base.util.impl.TodayUtilsImpl
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

class SparkImplFake(spark: SparkSession) extends ISpark {
  override def save(dataFrame: DataFrame, tableName: String): Unit = {

    println("gravando... FAKE")

    dataFrame.show(100, false)

  }

  override def get(columns: Array[String], tableName: DataFrame, partitionName: String, partitions: Array[String]): DataFrame = {
    val file = new File("src/test/resources/mock_example_20220812.csv")
    val fileAux = new File(file.getAbsolutePath)
    val path = fileAux.getAbsolutePath

    val spark = new SparkSessionServices().connectDevLocal

    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(path)

    dataFrameExample.withColumn("data", lit(new TodayUtilsImpl().getTodayOnlyNumbers()))
  }

  override def exportFile(dataFrame: DataFrame, format: String, pathFileName: String): Unit = {
    println("export file FAKE...")

    dataFrame.show(100, false)
  }

  override def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String, schema: StructType): DataFrame = {
    spark.read.format(format)
      .option("encoding", "UTF-8")
      .option("header", if (header) "true" else "false")
      .option("mode", "PERMISSIVE")
      .option("delimiter", delimiter)
      .schema(schema)
      .load(pathFileName)
      .cache()
  }

  override def getFile(pathFileName: String, format: String, map: Map[String, String], schema: StructType): DataFrame = {

    spark.read.format(format).options(map).schema(schema).load(pathFileName).cache()
  }

  override def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String): DataFrame = {
    val file = new File("src/test/resources/mock_example_20220812.csv")
    val fileAux = new File(file.getAbsolutePath)
    val path = fileAux.getAbsolutePath

    val spark = new SparkSessionServices().connectDevLocal

    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(path)

    dataFrameExample.withColumn("data", lit(new TodayUtilsImpl().getTodayOnlyNumbers()))


  }

  override def getHiveSchema(tableName: String, partition: String, timestampColumn: String): StructType = {
    ExampleBaseInterna.exampleTableInternalSchema
  }
}
