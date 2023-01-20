package ingestion

import ingestion.base.services.SparkSessionServices
import ingestion.model.ExampleDataFrame
import ingestion.util.impl.TodayUtilsImpl
import org.apache.spark.sql.functions.lit
import org.junit.{Before, Test}

import java.io.File
import java.time.LocalDate

class SparkLocalTest {

  private var PATH: String = null

  @Before
  def configMocks(): Unit = {
    val file = new File("src/test/resources/mock_example_20220812.csv")
    val fileAux = new File(file.getAbsolutePath)
    PATH = fileAux.getAbsolutePath
  }


  @Test def testSparkLocal(): Unit = {
    val spark = new SparkSessionServices().devLocal

    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(PATH)

    val result = dataFrameExample.withColumn("data", lit(LocalDate.now().toString))

    result.show(10, false)
  }

  @Test def test(): Unit = {
    val spark = new SparkSessionServices().devLocal

    val exampleDataFrame = new ExampleDataFrame("alex", "30", "11111111", new TodayUtilsImpl().getToday())

    import spark.implicits._

    val exampleDf = spark.createDataset(Seq(exampleDataFrame))

    exampleDf.toDF().show(100, false)

  }
}
