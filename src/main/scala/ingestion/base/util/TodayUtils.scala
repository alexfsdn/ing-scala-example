package ingestion.base.util

trait TodayUtils {

  /** *
   * YYYY-mm-DD
   *
   * @return
   */
  def getToday(): String

  /** *
   * YYYYmmDD
   *
   * @return
   */
  def getTodayOnlyNumbers(): String

  /** *
   * YYYYmm
   *
   * @return
   */
  def getTodayOnlyYearMonth(): String


  /** *
   * yyyy-MM-dd HH:mm:ss
   *
   * @return
   */
  def getTodayWithHours(): String


}
