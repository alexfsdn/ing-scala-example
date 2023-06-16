package ingestion.utils

import ingestion.util.CaptureParition
import org.junit.Test

class CapturePartitionTest {

  @Test def capturePartitionTest(): Unit = {
    println("CapturePartitionTest test...")

    val fileName = "/hadoop/input/example/example_name_arquivo_20220211.csv"
    val fileName2 = "\\hadoop\\input\\example\\example_name_arquivo_20220211.csv"
    val fileNameYearMonth = "/hadoop/input/example/example_name_arquivo_202202.csv"

    val partitionName = CaptureParition.captureParition(fileName)
    val partitionName2 = CaptureParition.captureParition(fileName2)
    val partitionNameYearhMonth = CaptureParition.captureParition(fileNameYearMonth)

    println("partitionName=" + partitionName)
    println("partitionName2=" + partitionName2)
    println("partitionNameYearhMonth=" + partitionNameYearhMonth)

    assert(partitionName == "20220211")
    assert(partitionNameYearhMonth == "202202")

  }

  @Test def onlyNameTest(): Unit = {
    println("onlyName Test test...")

    val pathFile = "/hadoop/input/example/example_name_arquivo_20220211.csv"
    val pathFile2 = "\\src\\test\\resources\\mock_example_20220812.csv".replace("\\", "/")
    val pathFileYearMonth = "/hadoop/input/example/example_name_arquivo_202202.csv"

    val onlyName = CaptureParition.getOnlyNameFile(pathFile)
    val onlyName2 = CaptureParition.getOnlyNameFile(pathFile2)
    val onlyNameYearhMonth = CaptureParition.getOnlyNameFile(pathFileYearMonth)

    println("onlyName=" + onlyName)
    println("onlyName2=" + onlyName2)
    println("onlyNameYearhMonth=" + onlyNameYearhMonth)

    assert(onlyName == "example_name_arquivo_20220211.csv")
    assert(onlyName2 == "mock_example_20220812.csv")
    assert(onlyNameYearhMonth == "example_name_arquivo_202202.csv")

  }
}
