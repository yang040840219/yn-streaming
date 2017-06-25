import java.util.Properties

import common.{DateTime, DBUtil, DateUtil}
import kafka.trans_event.BeepertfTransEvent._
import kafka.{BeeperTransEventDevelopEnvState, EnvState, AbstractConfEnv, SparkSessionSingleton}
import kafka.trans_event.BeepertfMessage
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{IntegerType, DataType}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/6/20.
  */
class SparkSuite extends FunSuite with Matchers with BeforeAndAfter with AbstractConfEnv {


    /**
      * 用来设置读取配置文件
      *
      * @return
      */
    override def getEnv: EnvState = BeeperTransEventDevelopEnvState

    val messages =
        """
          |{"msgid":4273351939,"data":{"event_type":31,"database":"beeper_trans_event","timestamp":"2017-06-19 01:00:03","storage":"mysql","values":{"customer_price":1,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-19 01:00:02","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[1]","driver_id":2269128,"project_type":2,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"\u8fd0\u8d39","is_addition":0,"customer_id":1985,"type":100,"status":400,"is_wrong_location":0,"addition_count":0,"bu_leader_id":1000450,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":2,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569},"table":"yn_trans_event","type":"UPDATE","before":{"customer_price":22500,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-12 19:16:39","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[]","driver_id":2269128,"project_type":0,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"","is_addition":0,"customer_id":1985,"type":100,"status":100,"is_wrong_location":0,"addition_count":0,"bu_leader_id":0,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":0,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569}}}@
          |{"msgid":4273351939,"data":{"event_type":31,"database":"beeper_trans_event","timestamp":"2017-06-19 01:00:03","storage":"mysql","values":{"customer_price":2,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-19 01:00:02","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[1]","driver_id":2269128,"project_type":2,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"\u8fd0\u8d39","is_addition":0,"customer_id":1985,"type":100,"status":800,"is_wrong_location":0,"addition_count":0,"bu_leader_id":1000450,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":2,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569},"table":"yn_trans_event","type":"UPDATE","before":{"customer_price":22500,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-12 19:16:39","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[]","driver_id":2269128,"project_type":0,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"","is_addition":0,"customer_id":1985,"type":100,"status":100,"is_wrong_location":0,"addition_count":0,"bu_leader_id":0,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":0,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569}}}@
          |{"msgid":4273351939,"data":{"event_type":31,"database":"beeper_trans_event","timestamp":"2017-06-19 01:00:03","storage":"mysql","values":{"customer_price":3,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-19 01:00:02","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[1]","driver_id":2269128,"project_type":2,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"\u8fd0\u8d39","is_addition":0,"customer_id":1985,"type":100,"status":900,"is_wrong_location":0,"addition_count":0,"bu_leader_id":1000450,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":2,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569},"table":"yn_trans_event","type":"UPDATE","before":{"customer_price":22500,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-12 19:16:39","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[]","driver_id":2269128,"project_type":0,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"","is_addition":0,"customer_id":1985,"type":100,"status":100,"is_wrong_location":0,"addition_count":0,"bu_leader_id":0,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":0,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569}}}@
          |{"msgid":4273351939,"data":{"event_type":31,"database":"beeper_trans_event","timestamp":"2017-06-19 01:00:03","storage":"mysql","values":{"customer_price":4,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-19 01:00:02","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[1]","driver_id":2269128,"project_type":2,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"\u8fd0\u8d39","is_addition":0,"customer_id":1985,"type":100,"status":400,"is_wrong_location":0,"addition_count":0,"bu_leader_id":1000450,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":2,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569},"table":"yn_trans_event","type":"UPDATE","before":{"customer_price":22500,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-12 19:16:39","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[]","driver_id":2269128,"project_type":0,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"","is_addition":0,"customer_id":1985,"type":100,"status":100,"is_wrong_location":0,"addition_count":0,"bu_leader_id":0,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":0,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569}}}
        """.stripMargin


    val sparkConf = new SparkConf().setAppName("SparkSuite")
    sparkConf.setMaster("local[1]")
    sparkConf.set("spark.sql.shuffle.partitions","3")

    val sparkContext = new SparkContext(sparkConf)

    val sparkSession = SparkSessionSingleton.getInstance(sparkConf)

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    test("save df to mysql"){

        val beeperMessages =  messages.split("@").map( x => {

           BeepertfMessage.parseMessage(x).get
        })

        val df = sparkContext.parallelize(beeperMessages)
        .map(line => {
            line.copy(timestamp = DateUtil.getNextTenMinute(DateUtil.str2mills(line.timestamp)))
        }).toDF()

        df.printSchema()
        println(df.count())

        val countDriverDataFrame = df.groupBy($"timestamp".as("run_time"),$"adcId".as("adc_id")).agg(
            countDistinct(when($"status" === "400",$"driverId").otherwise(null)).cast(IntegerType).as("sign_driver"),
            countDistinct(when($"status" === "800",$"driverId").otherwise(null)).cast(IntegerType).as("run_driver"),
            countDistinct(when($"status".isin("450","500","600","950"),$"driverId").otherwise(null)).cast(IntegerType).as("exception_driver"),
            countDistinct(when($"status" === "900",$"driverId").otherwise(null)).cast(IntegerType).as("complete_driver"),
            sum(when($"status" === "900",$"eventPrice").otherwise(0)).cast(IntegerType).as("event_price"),
            current_timestamp().as("created_at"),
            current_timestamp().as("updated_at")
        )


        countDriverDataFrame.show()

       // println(countDriverDataFrame.show())

//        df.createOrReplaceTempView("view_event_data")
//
//        // 10min 签到司机,在跑司机,配送完成司机,异常司机,基础运力价格
//        val sql =
//            """
//              | select
//              |   adcId as adc_id,
//              |   timestamp as run_time,
//              |   count(distinct(if(status='400',driverId,null))) as sign_driver,
//              |   count(distinct(if(status='800',driverId,null))) as run_driver,
//              |   count(distinct(if(status='900',driverId,null))) as complete_driver,
//              |   count(distinct(if(status in ('450','500','600','950'),driverId,null))) as exception_driver,
//              |   sum(if(status = '900',eventPrice,0)) as event_price,
//              |   current_timestamp as created_at,
//              |   current_timestamp as updated_at
//              |   from view_event_data where isDel = '0' group by adcId,timestamp
//            """.stripMargin
//
//        val countDriverDataFrame = sparkSession.sql(sql)

//        val url = conf.getString("mysql.url")
//        val table = "bi_stream_trans_event_ten_minute"
//        val userName = conf.getString("mysql.userName")
//        val password = conf.getString("mysql.password")
//        val properties = new Properties()
//        properties.put("user",userName)
//        properties.put("password",password)
        //countDriverDataFrame.write.mode(SaveMode.Append).jdbc(url,table,properties)

        val table = "bi_stream_trans_event_ten_minute"
        val url = conf.getString("mysql.url")
        val userName = conf.getString("mysql.userName")
        val password = conf.getString("mysql.password")

        val connection = DBUtil.createMySQLConnectionFactory(url, userName, password)
        val updateSQL =
            s"""
               | update $table
               |     set
               |         sign_driver = sign_driver + ? ,
               |         run_driver = run_driver + ? ,
               |         complete_driver = complete_driver + ? ,
               |         exception_driver = exception_driver + ? ,
               |         event_price = event_price + ?
               | where run_time = ? and adc_id = ?
                """.stripMargin
        val querySQL =
            s"""
               | select run_time,adc_id from $table where run_time = ? and adc_id = ?
                """.stripMargin
        countDriverDataFrame.collect().foreach(row => {
            val valueMap = row.getValuesMap(countDriverDataFrame.schema.map(_.name))
            val runTime = valueMap.getOrElse("run_time", "")
            val adcId = valueMap.getOrElse("adc_id", "0").toInt
            val signDriver = valueMap.getOrElse("sign_driver", 0)
            val runDriver = valueMap.getOrElse("run_driver", 0)
            val completeDriver = valueMap.getOrElse("complete_driver", 0)
            val exceptionDriver = valueMap.getOrElse("exception_driver", 0)
            val eventPrice = valueMap.getOrElse("event_price", 0)
            val count = DBUtil.runQuerySQLCount(connection, querySQL, Seq(runTime, adcId))
            if (count == 0) {
                DBUtil.insertTable(connection, valueMap, table)
            } else {
                DBUtil.runSQL(connection, updateSQL, Seq(signDriver, runDriver, completeDriver,
                    exceptionDriver, eventPrice, runTime, adcId
                ))
            }
        })

        }

}
