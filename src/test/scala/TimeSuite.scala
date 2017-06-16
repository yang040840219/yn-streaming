import java.io.File
import java.util.{Calendar, Date}

import common.{DateTime, DateUtil}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/14.
  */
class TimeSuite extends FunSuite with Matchers with BeforeAndAfter{

  test("next five minute") {
      // 2017-04-14 11:01  -> 11:05
      // 2017-04-14 11:02  -> 11:05

     val clock = new Date()

     val period = 600 * 1000

    val next = (math.floor(clock.getTime().toDouble / period) + 1).toLong * period

    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(next)
    println(calendar.getTime)

  }

  test("dateutil ten minute"){
    val clock = new Date()
     val next = DateUtil.getNextTenMinute(clock.getTime)
     println(next)
  }

  test("str 2 mills"){
     val str = "2017-04-14 12:01:39"
     val mills = DateUtil.str2mills(str)
     val minute = DateUtil.getNextTenMinute(mills)
     val dateTime = DateUtil.dateStr2DateTime(minute)
     println(dateTime.getDay)
  }

  test("file") {
    println(File.separator)
  }

  test("DateTime") {

    val dateTime = DateTime("2017-02-12_14-20",DateTime.DATETIMEMINUTE)

    println(dateTime)


  }

}
