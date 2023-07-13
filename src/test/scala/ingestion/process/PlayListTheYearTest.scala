package ingestion.process

import ingestion.base.enums.StatusEnums
import ingestion.base.services.SparkSessionServices
import ingestion.fake.SparkImplFake
import ingestion.util.impl.ValidParamUtilsImpl
import ingestion.util.{ValidParamUtils}
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, times, verify, when}

class PlayListTheYearTest {

  private var PATH: String = null
  private var PATH_2: String = null

  private var spark: SparkSession = null
  private var valid: ValidParamUtils = null

  @Before
  def configMocks(): Unit = {
    PATH = "src/test/resources/mock_example_20220812.csv"
    PATH_2 = "src/test/resources/mock_example_playlist_music_20220812.csv"

    spark = SparkSessionServices.devLocal
  }

  @Test def Success(): Unit = {
    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(PATH)
    val dataFrameExample2 = spark.read.option("header", "true").option("delimiter", ";").csv(PATH_2)

    dataFrameExample.createOrReplaceTempView("user")
    dataFrameExample2.createOrReplaceTempView("playlist")

    val userTable = "user"
    val playListTable = "playList"
    val tableNameIngestion = ""
    val year = "2022"
    val labelPartition = "dat_partition"

    val valid = mock(classOf[ValidParamUtils])

    when(valid.dataBaseTableValid(userTable)).thenReturn(true)
    when(valid.dataBaseTableValid(playListTable)).thenReturn(true)
    when(valid.dataBaseTableValid(tableNameIngestion)).thenReturn(true)
    when(valid.isEmpty(year)).thenReturn(true)
    when(valid.isEmpty(labelPartition)).thenReturn(true)

    val iSpark = new SparkImplFake(spark)


    val status: Int = new PlayListTheYear(iSpark, valid, true).run(userTable, playListTable, tableNameIngestion, year, labelPartition)

    verify(valid, times(1)).dataBaseTableValid(userTable)
    verify(valid, times(1)).dataBaseTableValid(playListTable)
    verify(valid, times(1)).dataBaseTableValid(tableNameIngestion)
    verify(valid, times(1)).isEmpty(year)
    assert(status == StatusEnums.SUCCESS.id)
  }

  @Test def Failure(): Unit = {
    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(PATH)
    val dataFrameExample2 = spark.read.option("header", "true").option("delimiter", ";").csv(PATH_2)

    dataFrameExample.createOrReplaceTempView("user")
    dataFrameExample2.createOrReplaceTempView("playlist")

    val userTable = "user"
    val playListTable = "playList"
    val tableNameIngestion = ""
    val year = null
    val labelPartition = "dat_partition"

    valid = new ValidParamUtilsImpl

    val iSpark = new SparkImplFake(spark)

    val status: Int = new PlayListTheYear(iSpark,  valid).run(userTable, playListTable, tableNameIngestion, year, labelPartition)

    assert(status == StatusEnums.FAILURE.id)
  }


}
