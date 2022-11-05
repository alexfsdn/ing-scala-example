package ingestion.trigger

import ingestion.base.dados.{ISpark, Ihdfs}
import ingestion.base.dados.impl.{HdfsImpl, SparkImpl}
import ingestion.process.ProcessIngestion
import ingestion.base.services.SparkSessionServices
import ingestion.util.TodayUtils
import ingestion.util.impl.TodayUtilsImpl

import java.time.{Duration, Instant}
import java.util

object Trigger extends Serializable {

  def main(args: Array[String]): Unit = {
    val start = Instant.now()
    val spark = new SparkSessionServices().connectProd
    val iSpark = new SparkImpl(spark)
    val ihdfs = new HdfsImpl()

    val statusFinal = run(iSpark, ihdfs, new TodayUtilsImpl)

    val end = Instant.now()
    val duration = Duration.between(start, end)

    System.exit(statusFinal)
  }

  def run(iSpark: ISpark, ihdfs: Ihdfs, today: TodayUtils): Int = {

    //val statusList: util.List[Int] = new ProcessIngestion()

    return 1
  }
}
