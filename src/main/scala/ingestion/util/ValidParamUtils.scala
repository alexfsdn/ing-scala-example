package ingestion.util

trait ValidParamUtils {

  def dataBaseTableValid(value: String): Boolean

  def isEmpty(value: String): Boolean
}
