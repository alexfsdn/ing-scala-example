package ingestion.trigger

import ingestion.base.dados.{ISpark, Ihdfs}
import ingestion.base.dados.impl.{HdfsImpl, SparkImpl}
import ingestion.base.enums.StatusEnums
import ingestion.process.ProcessIngestion
import ingestion.base.services.SparkSessionServices
import ingestion.util.TodayUtils
import ingestion.util.impl.TodayUtilsImpl

import java.time.{Duration, Instant}
import java.util

object Trigger extends Serializable {

  def main(args: Array[String]): Unit = {
    val start = Instant.now()
    val spark = SparkSessionServices.prd
    val iSpark = new SparkImpl(spark)
    val ihdfs = new HdfsImpl()
    val today = new TodayUtilsImpl()

    val statusFinal = run(iSpark, ihdfs, today)

    val end = Instant.now()
    val duration = Duration.between(start, end)

    println(duration.toMinutes)

    System.exit(statusFinal)
  }

  def run(iSpark: ISpark, ihdfs: Ihdfs, today: TodayUtils): Int = {

    val statusList: util.List[Int] = new ProcessIngestion(iSpark, ihdfs, today).run()

    val statusFinal = StatusEnums.validStatus(statusList)

    statusFinal
  }
}
