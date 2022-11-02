package ingestion.utils

import ingestion.base.util.CaptureParition
import org.junit.Test

class CapturePartitionTest {

  @Test def test(): Unit = {
    println("CapturePartitionTest test...")

    val fileName = "/hadoop/input/example/example_name_arquivo_20220211.csv"
    val fileNameYearMonth = "/hadoop/input/example/example_name_arquivo_202202.csv"

    val partitionName = CaptureParition.captureParition(fileName)
    val partitionNameYearhMonth = CaptureParition.captureParition(fileNameYearMonth)

    println("partitionName=" + partitionName)
    println("partitionNameYearhMonth=" + partitionNameYearhMonth)
  }
}
