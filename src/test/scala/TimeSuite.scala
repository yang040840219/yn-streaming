import java.io.File
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import common.{DateTime, DateUtil}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.util.Random

/**
  * Created by yxl on 17/4/14.
  */
class TimeSuite extends FunSuite with Matchers with BeforeAndAfter{

  test("next five minute") {
      // 2017-04-14 11:01  -> 11:05
      // 2017-04-14 11:02  -> 11:05

     val clock = new Date()

     val period = 600 * 1000

    val next = (math.floor(clock.getTime().toDouble / period)).toLong * period

    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(next)
    println(calendar.getTime)

  }

  test("dateutil ten minute"){
      val calendar = Calendar.getInstance()
    (1 to 20000).foreach(x => {
        val r =
        calendar.set(Calendar.MINUTE, Random.nextInt(x % 60))
        TimeUnit.SECONDS.sleep(1)
        val clock = calendar.getTime
        val next = DateUtil.getNextTenMinute(clock.getTime)
        println(clock + "   " + next)
    })

  }

  test("str 2 mills"){
     val str = "2017-04-14 23:59:59"
     val mills = DateUtil.str2mills(str)
     val minute = DateUtil.getBeforeTenMinute(mills)
     val dateTime = DateUtil.dateStr2DateTime(minute)
     println(dateTime.getMinute)
  }

  test("file") {
    println(File.separator)
  }

  test("DateTime") {

    val dateTime = DateTime("2017-02-12_14-20",DateTime.DATETIMEMINUTE)

    println(dateTime)


  }

}
