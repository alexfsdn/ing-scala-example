package ingestion.process

import ingestion.base.config.Config
import ingestion.base.dados.{ISpark, Ihdfs}
import ingestion.base.services.SparkSessionServices
import ingestion.fake.SparkImplFake
import ingestion.util.TodayUtils
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, when}

class PlayList2022Test {

  private var PATH: String = null
  private var PATH_2: String = null

  private var today: TodayUtils = null
  private var spark: SparkSession = null

  private lazy val INVALID_LINES: String = "_corrupt_record"

  @Before
  def configMocks(): Unit = {
    PATH = "src/test/resources/mock_example_20220812.csv"
    PATH_2 = "src/test/resources/mock_example_playlist_music_20220812.csv"

    today = mock(classOf[TodayUtils])

    when(today.getTodayOnlyNumbers()).thenReturn("20220812")
    when(today.getTodayWithHours()).thenReturn("20220812T162015")
    when(today.getToday()).thenReturn("20220812")

    spark = new SparkSessionServices().devLocal
  }

  @Test def PlayList2022Success(): Unit = {
    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(PATH)
    val dataFrameExample2 = spark.read.option("header", "true").option("delimiter", ";").csv(PATH_2)

    dataFrameExample.createOrReplaceTempView("user")
    dataFrameExample2.createOrReplaceTempView("playlist")

    spark.sql("select * from user").show()
    spark.sql("select * from playlist").show()

    new PlayList2022(new SparkImplFake(spark), today).run()

  }

}
