package ingestion.util.impl

import ingestion.util.ValidParamUtils

class ValidParamUtilsImpl extends ValidParamUtils {

  def dataBaseTableValid(value: String): Boolean = {
    if (value == null || value.trim.isEmpty || !value.trim.contains(".")) return false

    true
  }

  def isEmpty(value: String): Boolean = {
    if (value == null || value.trim.isEmpty) return false

    true
  }
}
