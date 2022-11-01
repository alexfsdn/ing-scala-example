package ingestion.base.services

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionServices {

  def connectDevLocal: SparkSession = {
    try {
      val conf = new SparkConf().setAppName("App Name example dev")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.some.config.option", "some-value")

      val spark = SparkSession.builder().master("local")
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
}
