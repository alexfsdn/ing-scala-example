package ingestion.base.util.impl

import ingestion.base.util.TodayUtils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

class TodayUtilsImpl extends TodayUtils {
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
    val month = LocalDate.now().getMonthValue.toString
    year.concat(month)
  }

  /** *
   * yyyy-MM-dd HH:mm:ss
   *
   * @return
   */
  override def getTodayWithHours(): String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).replace(" ", "T").replace(":", "").replace("-", "")

}
