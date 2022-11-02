package ingestion.base.dados

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait ISpark {

  def save(dataFrame: DataFrame, tableName: String): Unit

  def get(columns: Array[String] = Array("*"), tableName: DataFrame, partitionName: String, partitions: Array[String] = Array("*")): DataFrame

  def exportFile(dataFrame: DataFrame, format: String, pathFileName: String): Unit

  def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String, schema: StructType): DataFrame

  def getFile(pathFileName: String,format:String, map: Map[String, String], schema: StructType): DataFrame

  def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String): DataFrame

  def getHiveSchema(tableName: String, partition: String, timestampColumn: String): StructType
}
