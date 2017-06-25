package common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.StringUtils

/**
  * Created by yxl on 17/4/14.
  */

case class DateTime(year: String, month: String, day: String, hour: String, minute: String) {
    def getDay = year + DateTime.DATESPLIT + month + DateTime.DATESPLIT + day

    def getHour = getDay + DateTime.HOURSPLIT + hour

    def getMinute = getHour + DateTime.DATESPLIT + minute

    def getDate = {
         val sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm")
         sdf.parse(getMinute)
    }
}

object DateTime {

    val DATETIMEHOUR = "DATETIMEHOUR"

    val DATETIMEMINUTE = "DATETIMEMINUTE"

    val DATESPLIT = "-"
    val HOURSPLIT = "_"

    def apply(dateTimeStr: String, dateTimeType: String): DateTime = {
        if (dateTimeType.equals(DATETIMEHOUR)) {
            // yyyy-MM-dd_HH
            val dateTimeArray = dateTimeStr.split(HOURSPLIT)
            val (year, month, day) = dateTimeArray(0).split(DATESPLIT).toSeq match {
                case Seq(year, month, day) => (year, month, day)
            }
            DateTime(year, month, day, dateTimeArray(1), "0")
        } else if (dateTimeType.equals(DATETIMEMINUTE)) {
            // yyyy-MM-dd_HH_mm
            val dateTimeArray = dateTimeStr.split(HOURSPLIT)
            val (year, month, day) = dateTimeArray(0).split(DATESPLIT).toSeq match {
                case Seq(year, month, day) => (year, month, day)
            }
            val (hour, minute) = dateTimeArray(1).split(DATESPLIT).toSeq match {
                case Seq(hour, minute) => (hour, minute)
            }
            DateTime(year, month, day, hour, minute)
        }
        else {
            throw new Exception(s"wrong datetime format $dateTimeStr")
        }

    }
}

object DateUtil {

    val MINUTE_FORMAT = "yyyy-MM-dd_HH-mm"

    val SECONDS_FORMAT = "yyyy-MM-dd HH:mm:ss"

    val DAY_FORMAT = "yyyy-MM-dd"

    val PERIOD_TEN = 600 * 1000

    def next(mills: Long, period: Long) = {
        (math.floor(mills.toDouble / period) + 1).toLong * period
    }

    def before(mills: Long, period: Long) = {
        (math.floor(mills.toDouble / period)).toLong * period
    }

    /**
      * 取周期时间下届
      * @param mills
      * @return
      */
    def getNextTenMinute(mills: Long): String = {
        try {
            val nextMills = next(mills, PERIOD_TEN)
            val calendar = Calendar.getInstance()
            calendar.setTimeInMillis(nextMills)
            val sdf = new SimpleDateFormat(MINUTE_FORMAT)
            sdf.format(calendar.getTime())
        } catch {
            case ex: Exception => {
                null
            }
        }
    }

    /**
      * 取周期时间上界
      * @param mills
      * @return
      */
    def getBeforeTenMinute(mills: Long): String = {
        try {
            val nextMills = before(mills, PERIOD_TEN)
            val calendar = Calendar.getInstance()
            calendar.setTimeInMillis(nextMills)
            val sdf = new SimpleDateFormat(MINUTE_FORMAT)
            sdf.format(calendar.getTime())
        } catch {
            case ex: Exception => {
                null
            }
        }
    }

    def str2mills(str: String): Long = {
        try {
            val sdf = new SimpleDateFormat(SECONDS_FORMAT)
            sdf.parse(str).getTime
        } catch {
            case ex: Exception => {
                0l
            }
        }
    }


    def getCurrent = Calendar.getInstance().getTime

    def formatInt(value: Int): String = {
        if (value < 10) "0" + value
        else value.toString
    }

    def dateStr2DateTime(dateStr: String): DateTime = {
        val sdf = new SimpleDateFormat(MINUTE_FORMAT)
        val date = sdf.parse(dateStr)
        val calendar = Calendar.getInstance()
        calendar.setTime(date)
        DateTime(formatInt(calendar.get(Calendar.YEAR)),
            formatInt(calendar.get(Calendar.MONTH) + 1),
            formatInt(calendar.get(Calendar.DAY_OF_MONTH)),
            formatInt(calendar.get(Calendar.HOUR_OF_DAY)),
            formatInt(calendar.get(Calendar.MINUTE)))
    }

    def getCurrentMills: Long = {
        new Date().getTime
    }

    def getDay: String = {
        val sdf = new SimpleDateFormat(DAY_FORMAT)
        sdf.format(new Date())
    }

}
