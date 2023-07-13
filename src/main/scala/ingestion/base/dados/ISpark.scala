package ingestion.base.dados

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait ISpark {

  def save(dataFrame: DataFrame, tableName: String): Unit

  def save(columns: Array[String], tableName: String, tableNameTmp: String, partitionName: String, partition: String): Unit

  def get(columns: Array[String] = Array("*"), tableName: String, partitionName: String, partitions: Array[String] = Array("*")): DataFrame

  def get(query: String): DataFrame

  def exportFile(dataFrame: DataFrame, format: String, pathFileName: String): Unit

  def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String, schema: StructType): DataFrame

  def getFile(pathFileName: String, format: String, map: Map[String, String], schema: StructType): DataFrame

  def getFile(pathFileName: String, format: String, header: Boolean, delimiter: String): DataFrame

  def getHiveSchema(tableName: String, partition: String, timestampColumn: String): StructType
}
