import java.sql.Timestamp
import java.util.Date

import com.typesafe.config.ConfigFactory
import common.{DateTime, DateUtil, DBUtil}
import kafka.AbstractConfEnv
import org.apache.kafka.common.TopicPartition
import org.scalatest.{Matchers, FunSuite, BeforeAndAfter}
import scala.collection.mutable.Map

/**
  * Created by yxl on 17/4/12.
  */
class SqlSuite extends FunSuite with Matchers with BeforeAndAfter with AbstractConfEnv {

  val topic = "state-direct-kafka"

  val offsetSQLConnection = DBUtil.createMySQLConnectionFactory(conf.getString("mysql.url"),
    conf.getString("mysql.userName"),conf.getString("mysql.password"))




  test("connection successful") {
     // println(offsetSQLConnection)
      println(new Timestamp(DateUtil.getCurrent.getTime))
  }


  test("map 2 table"){
      val time = "2017-06-19 01:00:03"
      val ten = DateUtil.getNextTenMinute(DateUtil.str2mills(time))
      val dateHour = DateUtil.dateStr2DateTime(ten).getHour
      val dateTime = DateTime(dateHour,DateTime.DATETIMEHOUR)
      println(s"dateTime:$dateTime")
      val deleteSQL = "delete from bi_stream_trans_event_one_hour where run_time = ?"
      DBUtil.runSQL(offsetSQLConnection,deleteSQL,Seq(dateTime.getDate))
      val sqlDate = new java.sql.Date(dateTime.getDate.getTime)
      val columnMap = Map[String,Any]("run_time" -> sqlDate,
          "sign_driver" -> 1,
          "run_driver" -> 2,
          "complete_driver" -> 3,
          "exception_driver" -> 4,
          "event_price" -> 5,
          "created_at" -> DateUtil.getCurrent,
          "updated_at" -> DateUtil.getCurrent).toMap

      println(columnMap)

      DBUtil.insertTable(offsetSQLConnection,columnMap,"bi_stream_trans_event_one_hour")

  }

    test("date"){
        val time = "2017-06-19 01:00:03"
        val ten = DateUtil.getNextTenMinute(DateUtil.str2mills(time))
        println(ten)
        val dateTime = DateUtil.dateStr2DateTime(ten)
        println(dateTime)
        println(dateTime.getDate)
        println(new Timestamp(dateTime.getDate.getTime))
    }

    test("minute"){
        val timeHour = "2017-06-19_01"
        val dateTime = DateTime(timeHour,DateTime.DATETIMEHOUR)
        println(s"dateTime:$dateTime" )
        val date = dateTime.getDate
        println(s"date:$date")
        val sqlDate = new java.sql.Date( date.getTime)
        println(s"sql.date:$sqlDate")
        println(new Timestamp(date.getTime))
    }

  test("ResultSet to List[Map[String,Object]]"){

    val sql = s"select topic,`partition`,`offset` from kafka_consumer_offset where topic ='$topic'"

    val statement = offsetSQLConnection.createStatement()

    val resultSet = statement.executeQuery(sql)

    val rowsSeq = DBUtil.resultSet2Seq(resultSet)
    val offset = rowsSeq match {
      case None => None
      case Some(rows) => {
        val offsetMap = rows.foldLeft(Map[TopicPartition, Long]())((map, row) => {
          val topic = row.getOrElse("topic", "").toString
          val partition = row.getOrElse("partition", -1)
          val offset = row.getOrElse("offset", -1)
          if (topic != null && partition != null && offset != null
            && partition.toString.toInt != -1 && offset.toString.toInt != -1) {
            val topicPartition = new TopicPartition(topic, partition.toString.toInt)
            map.+=(topicPartition -> offset.toString.toInt)
          }
          map
        })
        Some(offsetMap)
      }
    }

    print(offset.get)

  }

}
