package ingestion.base.services

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionServices {

  def devLocal: SparkSession = {
    try {
      val conf = new SparkConf().setAppName("App Name example dev")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.some.config.option", "some-value")

      val spark = SparkSession.builder().master("local[2]")
        .appName("spark local")
        .config(conf)
        .getOrCreate()
      spark
    } catch {
      case e: Exception =>
        print("erro ao tentar criar uma sessão com o spark")
        null
    }
  }

  def devLocalEnableHiveSupport: SparkSession = {
    try {

      val conf = new SparkConf().setAppName("App Name example prod")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.some.config.option", "some-value")
        .set("spark.sql.catalogImplementation", "hive")

      val spark = SparkSession.builder()
        .appName("test")
        .master("local[*]")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()

      spark

    } catch {
      case e: Exception =>
        print("erro ao tentar criar uma sessão com o spark")
        null
    }
  }


  def prd: SparkSession = {
    try {
      val conf = new SparkConf().setAppName("App Name example prod")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.some.config.option", "some-value")

      val spark = SparkSession.builder().master("yarn")
        .appName("spark prd yarn")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      spark
    } catch {
      case e: Exception =>
        print("erro ao tentar criar uma sessão com o spark")
        null
    }
  }
}
