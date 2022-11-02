package ingestion.base.config

import com.typesafe.config.{Config, ConfigFactory}

object Config {

  private val config: Config = {
    try {

      ConfigFactory.load()
    } catch {
      case ex: Exception =>
        println("Falha ao carregar config")
        println(ex.getMessage)
        throw ex
    }
  }

  def getFileName: String = config.getString("file_name")

  def getDataBase: String = config.getString("database")

  def getJobName: String = config.getString("job_name")

  def getTable: String = config.getString("table")

  def getPartitionName: String = config.getString("col_partition_name")

  def getIngestionName: String = config.getString("col_ingestion_name")

  def getFormat: String = config.getString("format")

  def getMap: String = config.getString("map_list")

  def getColumnOrder: String = config.getString("col_order")

  def getUrlHdfs: String = config.getString("hdfs_url")

  def getInputPath: String = config.getString("input_path")

  def getArchiving: String = config.getString("archiving_path")

  def getArchivingError: String = config.getString("archiving_error_path")

}
