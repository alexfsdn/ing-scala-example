package ingestion.base.enums

import java.util

object StatusEnums extends Enumeration {
  type StatusEnums = Value
  val SUCCESS, FAILURE, THERE_IS_NOT_DATA_TO_PROCESS = Value

  def validStatus(statusList: util.List[Int]): Int = {
    if (statusList == null || statusList.size() <= 0) return FAILURE.id

    val success = statusList.contains(SUCCESS.id)
    val failure = statusList.contains(FAILURE.id)
    val notData = statusList.contains(THERE_IS_NOT_DATA_TO_PROCESS.id)

    if (success == false && failure == false && notData == false) {
      return FAILURE.id
    } else if (success == false && failure == true && notData == false) {
      return FAILURE.id
    } else if (success == false && failure == false && notData == true) {
      return THERE_IS_NOT_DATA_TO_PROCESS.id
    }

    StatusEnums.SUCCESS.id
  }
}
