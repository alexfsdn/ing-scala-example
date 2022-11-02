package ingestion.base.util

object CaptureParition {

  /** *
   *
   * @return
   */
  def captureParition(fileName: String): String = {
    fileName.split("\\.").head.split("_").last
  }

}
