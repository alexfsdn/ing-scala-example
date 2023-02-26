package ingestion.util

class ValidParamUtilsImpl extends ValidParamUtils {

  def dataBaseTableValid(value: String): Boolean = {
    if (value.trim.isEmpty) return false
    if (value.trim == null) return false
    if (!value.trim.contains(".") == null) return false

    true
  }
}