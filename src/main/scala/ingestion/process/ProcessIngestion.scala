package ingestion.process

import ingestion.base.config.{Config, Tables}
import ingestion.base.dados.{ISpark, Ihdfs}
import ingestion.base.enums.StatusEnums
import ingestion.util.impl.TodayUtilsImpl
import ingestion.util.{CaptureParition, TodayUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.dmg.pmml.False
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util

class ProcessIngestion(iSpark: ISpark, ihdfs: Ihdfs) {

  private var parameters: Map[String, String] = null
  private lazy val FORMAT: String = Config.getFormat
  private lazy val TABLE_NAME: String = Tables.TABLE_INGESTION
  private lazy val FILE_NAME: String = Config.getFileName
  private lazy val COL_ORDER: List[String] = Config.getColumnOrder
  private lazy val PARTITION_NAME: String = Config.getPartitionName
  private lazy val TIMESTAMP_NAME: String = Config.getIngestionName
  private lazy val INPUT_PATH: String = Config.getInputPath
  private lazy val ARCHIVING_PATH: String = Config.getArchiving
  private lazy val ARCHIVING_ERROR_PATH: String = Config.getArchivingError
  private lazy val JOB_NAME: String = Config.getJobName
  private lazy val INVALID_LINES: String = "_corrupt_record"
  private lazy val ORIGINAL_LABEL: String = "ORIGINAL"


  def run(): util.List[Int] = {

    println("Starting... ".concat(getClass.getSimpleName))

    initParameters

    println(s"Job execution $JOB_NAME")
    println("format=" + FORMAT)
    println("table=" + TABLE_NAME)
    println("file_name=" + FILE_NAME)
    println("col_order=" + COL_ORDER)
    println("input_path=" + INPUT_PATH)
    println("job_name=" + JOB_NAME)
    println("archiving=" + ARCHIVING_PATH)
    println("archiving_error=" + ARCHIVING_ERROR_PATH)
    println("partition_name=" + PARTITION_NAME)
    println("timestamp=" + TIMESTAMP_NAME)
    println("parameters=" + parameters.toString())
    println(s"Step 1... Listing all files to this process $INPUT_PATH")

    val status = new util.ArrayList[Int]

    val files = ihdfs.lsAll(INPUT_PATH)

    if (files == null || files.isEmpty) {
      println("There is no file to process")
      status.add(StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id)
      return status
    }

    println(s"Step 2... walked ont the list")

    files.foreach(pathFile => {

      if (pathFile.contains(FILE_NAME)) {
        println(s"Verifying transfer file done: $pathFile")

        val statusIngestion = process(pathFile)
        status.add(statusIngestion)
      }

    })

    status
  }

  def process(pathFile: String): Int = {
    try {
      println("Step 3.... capturing the file name")

      val fileName = CaptureParition.getOnlyNameFile(pathFile)

      val exist: Boolean = ihdfs.exist(pathFile)

      if (!exist) {
        println(s"The file not found $fileName")
        throw new NullPointerException(s"The file not found $fileName")
      }

      val schema = getHiveSchema(TABLE_NAME, PARTITION_NAME, TIMESTAMP_NAME, INVALID_LINES)

      println(s" Hive schema... ${schema.toString()}")

      println(s"Step 5... capturing file to validation: $fileName")

      val df: DataFrame = getFile(pathFile, FORMAT, parameters, schema).persist(StorageLevel.MEMORY_ONLY_SER)

      val totalLines = df.count()

      println(s"Step 6... capturing invalid lines")

      val dfInvalidLines: DataFrame = getInvalidLines(df, INVALID_LINES).persist(StorageLevel.MEMORY_ONLY_SER)

      val totalInvalidesLines = dfInvalidLines.count()

      println(s"... total invalid lines $totalInvalidesLines")

      if (totalLines == totalInvalidesLines) {
        println("There is no data to process")
        throw new NullPointerException(s"There is no data to process $fileName")
      }

      val partition = CaptureParition.captureParition(pathFile)
      val ingestionTimeStamp = TodayUtilsImpl.getTodayWithHours()

      println(s"Step 7... capturing valid lines")

      val dfValidLines: DataFrame = getValidLines(df, INVALID_LINES).persist(StorageLevel.MEMORY_ONLY_SER)

      df.unpersist()

      val totalValidesLines = dfValidLines.count()

      println(s"... total invalid lines $totalValidesLines")

      if (totalValidesLines <= 0) {
        println("There is no data to process")
        throw new NullPointerException(s"There is no data to process $fileName")
      }

      println(s"Step 8... ingestion data in the $TABLE_NAME")

      val tableNameTmp = "tableTMP".concat(ingestionTimeStamp);

      dfValidLines
        .withColumn(TIMESTAMP_NAME, lit(current_timestamp()))
        .select(COL_ORDER.map(col(_)): _*).createOrReplaceTempView(tableNameTmp)

      iSpark.save(COL_ORDER.toArray, TABLE_NAME, tableNameTmp, PARTITION_NAME, partition)

      val newName = fileName.concat(ORIGINAL_LABEL).concat(TodayUtilsImpl.getTodayWithHours())

      movingAchiving(pathFile, newName)

      if (dfValidLines.count() > 0) {
        println(s"Step 9... exporting valid lines to archiving path $ARCHIVING_PATH")
        export(dfValidLines, FORMAT, ARCHIVING_PATH.concat(fileName.replace(".csv", "_").concat(TodayUtilsImpl.getTodayWithHours()).concat(".csv")))
      }

      if (dfInvalidLines.count() > 0) {
        println(s"Step 10... exporting invalid lines to archiving path error $ARCHIVING_ERROR_PATH")
        export(dfInvalidLines, FORMAT, ARCHIVING_ERROR_PATH.concat(fileName.replace(".csv", "_").concat(TodayUtilsImpl.getTodayWithHours()).concat(".csv")))
      }

      dfValidLines.unpersist()
      dfInvalidLines.unpersist()

    } catch {
      case _: NullPointerException =>
        return StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id
      case _: Exception =>
        return StatusEnums.FAILURE.id
    }


    StatusEnums.SUCCESS.id
  }

  private def export(dataFrame: DataFrame, format: String, pathFile: String): Unit = {
    iSpark.exportFile(dataFrame, format, pathFile)
  }

  private def movingAchiving(pathFile: String, newName: String): Boolean = {
    ihdfs.mv(pathFile, ARCHIVING_PATH.concat(newName))
  }

  private def getHiveSchema(tableName: String, partitionName: String, timestampName: String, invalidLines: String): StructType = {
    iSpark.getHiveSchema(tableName, partitionName, timestampName).add(invalidLines, StringType, true)
  }

  def initParameters: Unit = {
    implicit val formats = org.json4s.DefaultFormats
    parameters = parse(Config.getMap).extract[Map[String, String]]
  }

  def getFile(pathFile: String, format: String, parameters: Map[String, String], schema: StructType): DataFrame = {
    iSpark.getFile(pathFile, format, parameters, schema)
  }

  def getInvalidLines(df: DataFrame, invalidLinesColName: String): DataFrame = {
    df.filter(col(invalidLinesColName).isNotNull)
      .select(col(invalidLinesColName))
  }

  def getValidLines(df: DataFrame, invalidLinesColName: String): DataFrame = {
    df.filter(col(invalidLinesColName).isNull).drop(col(invalidLinesColName))
  }

}