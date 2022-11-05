package ingestion.process

import ingestion.base.config.{Config, Tables}
import ingestion.base.dados.{ISpark, Ihdfs}
import ingestion.base.enums.StatusEnums
import ingestion.util.{CaptureParition, TodayUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util

class ProcessIngestion(iSpark: ISpark, ihdfs: Ihdfs, today: TodayUtils) {

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
  private lazy val INVALID_LINES: String = "_invalid_lines"
  private lazy val ORIGINAL_LABEL: String = "ORIGINAL"


  def run(): util.List[Int] = {

    println("Starting...")
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

        val statusIngestion = processIngestion(pathFile)
        status.add(statusIngestion)
      }

    })

    status
  }

  def processIngestion(pathFile: String): Int = {
    try {
      println("Step 3.... capturing the file name")

      val fileName = CaptureParition.captureParition(pathFile)

      val exist: Boolean = ihdfs.exist(pathFile)

      if (!exist) {
        println(s"The file not found $fileName")
        throw new NullPointerException(s"The file not found $fileName")
      }

      val schema = getHiveSchema(TABLE_NAME, PARTITION_NAME, TIMESTAMP_NAME, INVALID_LINES)

      println(s" Hive schema... ${schema.toString()}")

      println(s"Step 5... capturing file to validation: $fileName")

      val df: DataFrame = getFile(pathFile, FORMAT, parameters, schema)

      val dfInvalidLines: DataFrame = getInvalidLines(df, INVALID_LINES).persist

      val partitionName = CaptureParition.getOnlyNameFile(pathFile)
      val ingestionTimeStamp = today.getToday()

      val dfValidLines: DataFrame = getValidLines(df, INVALID_LINES).persist

      if (dfValidLines.count() <= 0) {
        println("...")
        throw new NullPointerException(s"There is no data to process $fileName")
      }

      val dfToSave = dfValidLines
        .withColumn(TIMESTAMP_NAME, lit(ingestionTimeStamp))
        .withColumn(PARTITION_NAME, lit(partitionName))
        .select(COL_ORDER.map(col(_)): _*).persist()

      iSpark.save(dfToSave, TABLE_NAME)
      dfToSave.unpersist()

      val newName = fileName.concat(ORIGINAL_LABEL).concat(today.getTodayWithHours())

      movingAchiving(pathFile, newName)

      if (dfValidLines.count() > 0) {
        println("...")
        export(dfValidLines, FORMAT, ARCHIVING_PATH.concat(fileName.concat(today.getTodayWithHours())))
      }

      if (dfInvalidLines.count() > 0) {
        println("...")
        export(dfInvalidLines, FORMAT, ARCHIVING_ERROR_PATH.concat(fileName.concat(today.getTodayWithHours())))
      }

    } catch {
      case _: NullPointerException =>
        return StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id
      case _: Exception =>
        return StatusEnums.FAILURE.id
    }

    println("...")

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
