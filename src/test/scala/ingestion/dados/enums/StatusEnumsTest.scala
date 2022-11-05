package ingestion.dados.enums

import ingestion.base.enums.StatusEnums
import org.junit.Test

import java.util
import java.util._

class StatusEnumsTest {

  @Test def successTest(): Unit = {
    println("successTest test...")

    val success: Int = StatusEnums.SUCCESS.id

    val statusList: util.List[Int] = Arrays.asList(success, success, success)

    val statusFinal = StatusEnums.validStatus(statusList)

    println("statusFinal=" + statusFinal)
    assert(statusFinal == StatusEnums.SUCCESS.id)

  }

  @Test def successWhenHaveSuccessAndNoDataTest(): Unit = {
    println("successWhenHaveSuccessAndNoDataTest test...")

    val success: Int = StatusEnums.SUCCESS.id
    val notData: Int = StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id

    val statusList: util.List[Int] = Arrays.asList(success, success, notData)

    val statusFinal = StatusEnums.validStatus(statusList)

    println("statusFinal=" + statusFinal)
    assert(statusFinal == StatusEnums.SUCCESS.id)

  }

  @Test def failureWhenListIsEmptyTest(): Unit = {
    println("failureWhenListIsEmptyTest test...")

    val statusList: util.List[Int] = Arrays.asList()

    val statusFinal = StatusEnums.validStatus(statusList)

    val statusList2: util.List[Int] = null

    val statusFinal2 = StatusEnums.validStatus(statusList2)

    println("statusFinal=" + statusFinal)
    println("statusFinal2=" + statusFinal2)

    assert(statusFinal == StatusEnums.FAILURE.id)
    assert(statusFinal2 == StatusEnums.FAILURE.id)

  }

  @Test def failureTest(): Unit = {

    val failure: Int = StatusEnums.FAILURE.id

    val statusList: util.List[Int] = Arrays.asList(failure, failure, failure)

    val statusFinal = StatusEnums.validStatus(statusList)

    println("statusFinal=" + statusFinal)

    assert(statusFinal == StatusEnums.FAILURE.id)

  }

  @Test def noDateTest(): Unit = {
    val notData: Int = StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id

    val statusList: util.List[Int] = Arrays.asList(notData, notData, notData)

    val statusFinal = StatusEnums.validStatus(statusList)

    println("statusFinal=" + statusFinal)

    assert(statusFinal == StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id)
  }

  @Test def failureWhenHaveFailureAndNoDataTest(): Unit = {
    val failure: Int = StatusEnums.FAILURE.id
    val notData: Int = StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id

    val statusList: util.List[Int] = Arrays.asList(failure, notData, notData)

    val statusFinal = StatusEnums.validStatus(statusList)

    println("statusFinal=" + statusFinal)

    assert(statusFinal == StatusEnums.FAILURE.id)

  }

  @Test def successWhenHaveSuccessAndFailureTest(): Unit = {
    val success: Int = StatusEnums.SUCCESS.id
    val failure: Int = StatusEnums.FAILURE.id

    val statusList: util.List[Int] = Arrays.asList(success, failure, failure)

    val statusFinal = StatusEnums.validStatus(statusList)

    println("statusFinal=" + statusFinal)

    assert(statusFinal == StatusEnums.SUCCESS.id)
  }
}
