package ingestion.util

object CaptureParition {

  /** *
   *
   * @return
   */
  def captureParition(fileName: String): String = {
    fileName.split("\\.").head.split("_").last
  }

  def getOnlyNameFile(pathFile: String) = {
    pathFile.split("/").last
  }

}
