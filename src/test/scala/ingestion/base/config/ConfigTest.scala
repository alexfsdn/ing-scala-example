package ingestion.base.config

import org.junit.Test

import org.json4s._
import org.json4s.jackson.JsonMethods._

class ConfigTest {

  @Test def testMap(): Unit = {
    implicit val formats = org.json4s.DefaultFormats

    val result: Map[String, String] = parse(Config.getMap).extract[Map[String, String]]

    result.foreach(a => println(a))
  }

  @Test def test(): Unit = {
    println("config...")
    println(".........")
    println(".........")

    println("file_name=" + Config.getFileName)
    println(".........")
    println(".........")

    println("database=" + Config.getDataBase)

    println(".........")
    println(".........")

    println("job_name=" + Config.getJobName)

    println(".........")
    println(".........")

    println("table=" + Config.getTable)

    println(".........")
    println(".........")

    println("col_partition_name=" + Config.getPartitionName)


    println(".........")
    println(".........")

    println("col_ingestion_name=" + Config.getIngestionName)


    println(".........")
    println(".........")

    println("format=" + Config.getFormat)

    println(".........")
    println(".........")

    println("database_control=" + Config.getDataBaseControl)


    println(".........")
    println(".........")

    println("table_control=" + Config.getTableControl)


    println(".........")
    println(".........")

    println("col_order=" + Config.getColumnOrder)

    println(".........")
    println(".........")

    println("col_order_table_control=" + Config.getTableControl)


    println(".........")
    println(".........")

    println("hdfs_url=" + Config.getUrlHdfs)


    println(".........")
    println(".........")

    println("input_path=" + Config.getInputPath)


    println(".........")
    println(".........")

    println("archiving_path=" + Config.getArchiving)


    println(".........")
    println(".........")

    println("archiving_error_path=" + Config.getArchivingError)
  }
}
