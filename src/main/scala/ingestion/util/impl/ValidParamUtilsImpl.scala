package ingestion.util.impl

import ingestion.util.ValidParamUtils

class ValidParamUtilsImpl extends ValidParamUtils {

  def dataBaseTableValid(value: String): Boolean = {
    if (value.trim.isEmpty) return false
    if (value.trim == null) return false
    if (!value.trim.contains(".")) return false

    true
  }
}
