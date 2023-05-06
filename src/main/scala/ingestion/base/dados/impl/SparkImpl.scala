package ingestion.base.dados.impl

import ingestion.base.dados.ISpark
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

class SparkImpl(spark: SparkSession) extends ISpark with Serializable {

  /** *
   *
   * @param dataFrame
   * @param tableName
   */
  override def save(dataFrame: DataFrame, tableName: String): Unit = {

    try {

      dataFrame.write.format("orc").mode(SaveMode.Overwrite)
        .option("partitionOverwriteMode", "dynamic")
        .insertInto(s"${tableName}")

    } catch {
      case ex: Exception =>
        println("Filed trying to write record")
        println(ex.getMessage)
        throw ex
    }

  }

  /** *
   *
   * @param columns
   * @param tableName
   * @param partitionName
   * @param partitions
   * @return
   */
  override def get(columns: Array[String], tableName: String, partitionName: String, partitions: Array[String]): DataFrame = {
    try {

      val sqlCommand =
        s"""
           |SELECT ${columns.mkString(",")}
           |FROM ${tableName}
           |WHERE ${partitionName} IN (${partitions.mkString(",")})
           |""".stripMargin

      spark.sql(sqlCommand)

    } catch {
      case ex: Exception =>
        println("Filed to query")
        println(ex.getMessage)
        throw ex
    }


  }

  /** *
   *
   * @param dataFrame
   * @param format
   * @param pathFileName
   */
  override def exportFile(dataFrame: DataFrame, format: String, pathFileName: String): Unit = {
    dataFrame.write.format(format).save(pathFileName)
  }

  /** *
   *
   * @param pathFileName
   * @param format
   * @param header
   * @param delimiter
   * @param schema
   * @return
   */
  override def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String, schema: StructType): DataFrame = {
    spark.read.format(format)
      .option("encoding", "UTF-8")
      .option("header", header)
      .option("mode", "PERMISSIVE")
      .option("delimiter", delimiter)
      .schema(schema)
      .load(pathFileName)
      .cache()
  }

  /** *
   *
   * @param pathFileName
   * @param format
   * @param map
   * @param schema
   * @return
   */
  override def getFile(pathFileName: String, format: String, map: Map[String, String], schema: StructType): DataFrame = {
    spark.read.format(format).options(map).option("mode", "PERMISSIVE") //The PERMISSIVE mode sets to null field values when corrupted records are detected. By default, if you don't specify the parameter mode, Spark sets the PERMISSIVE value.
      .schema(schema).load(pathFileName).cache()
  }

  /** *
   *
   * @param pathFileName
   * @param format
   * @param header
   * @param delimiter
   * @return
   */
  override def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String): DataFrame = {
    spark.read.format(format)
      .option("encoding", "UTF-8")
      .option("header", header)
      .option("mode", "PERMISSIVE")
      .option("delimiter", delimiter)
      .load(pathFileName)
  }

  /** *
   *
   * @param tableName
   * @param partition
   * @param timestampColumn
   * @return
   */
  override def getHiveSchema(tableName: String, partition: String, timestampColumn: String): StructType = {
    spark.table(tableName).drop(partition).drop(timestampColumn).schema
  }

  override def get(query: String): DataFrame = {
    spark.sql(query)
  }
}
