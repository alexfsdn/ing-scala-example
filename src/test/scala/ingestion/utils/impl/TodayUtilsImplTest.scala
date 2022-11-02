package ingestion.utils.impl

import ingestion.base.util.impl.TodayUtilsImpl
import org.junit.Test

class TodayUtilsImplTest {

  @Test def test(): Unit = {
    println("today test...")

    val todayImpl = new TodayUtilsImpl()

    val today = todayImpl.getToday()
    val todayOnlyNumbers = todayImpl.getTodayOnlyNumbers()
    val yearMonth = todayImpl.getTodayOnlyYearMonth()

    println("today=" + today)
    println("todayOnlyNumbers=" + todayOnlyNumbers)
    println("yearMonth=" + yearMonth)

    assert(today.contains("-"))
    assert(!todayOnlyNumbers.contains("-"))
    assert(yearMonth.size == 6)

  }
}
