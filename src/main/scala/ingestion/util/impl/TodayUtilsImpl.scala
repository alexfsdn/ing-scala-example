package ingestion.util.impl

import ingestion.util.TodayUtils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

object TodayUtilsImpl extends TodayUtils {
  /** *
   * YYYY-mm-DD
   *
   * @return
   */
  override def getToday(): String = {
    LocalDate.now().toString
  }

  /** *
   * YYYYmmDD
   *
   * @return
   */
  override def getTodayOnlyNumbers(): String = {
    LocalDate.now().toString.replace("-", "")
  }

  /** *
   * YYYYmm
   *
   * @return
   */
  override def getTodayOnlyYearMonth(): String = {
    val year = LocalDate.now().getYear.toString
    var month = LocalDate.now().getMonthValue.toString
    if (month.size == 1) {
      month = "0".concat(month)
    }
    year.concat(month)
  }

  /** *
   * yyyy-MM-dd HH:mm:ss
   *
   * @return
   */
  override def getTodayWithHours(): String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).replace(" ", "T").replace(":", "").replace("-", "")

}
