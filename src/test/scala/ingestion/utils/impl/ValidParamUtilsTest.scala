package ingestion.utils.impl

import ingestion.util.impl.ValidParamUtilsImpl
import org.junit.Test

class ValidParamUtilsTest {


  @Test def test(): Unit = {
    println("ValidParamUtilsTest test...")

    val valid = new ValidParamUtilsImpl

    assert(valid.dataBaseTableValid("music.play") == true)
    assert(valid.dataBaseTableValid("musicplay") == false)
    assert(valid.dataBaseTableValid(null) == false)
    assert(valid.dataBaseTableValid("") == false)

  }

}
