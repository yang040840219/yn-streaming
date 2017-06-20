import kafka.trans_event.BeepertfMessage
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by yxl on 17/4/14.
  */
class MessageSuite extends FunSuite with Matchers with BeforeAndAfter {

    val message =
        """
          |{"msgid":4273351939,"data":{"event_type":31,"database":"beeper_trans_event","timestamp":"2017-06-19 01:00:03","storage":"mysql","values":{"customer_price":22500,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-19 01:00:02","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[1]","driver_id":2269128,"project_type":2,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"\u8fd0\u8d39","is_addition":0,"customer_id":1985,"type":100,"status":400,"is_wrong_location":0,"addition_count":0,"bu_leader_id":1000450,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":2,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569},"table":"yn_trans_event","type":"UPDATE","before":{"customer_price":22500,"work_begin_time":"1900-01-01 03:00:00","is_del":0,"task_is_back":0,"updated_at":"2017-06-12 19:16:39","fcc_id":0,"trans_task_id":1595654,"new_trade_level_two":"[]","driver_id":2269128,"project_type":0,"sales_id":1000736,"is_supplement":0,"is_late":0,"id":5642840,"task_type":200,"complete_time":null,"car_team_id":0,"invoice_contents":"","is_addition":0,"customer_id":1985,"type":100,"status":100,"is_wrong_location":0,"addition_count":0,"bu_leader_id":0,"work_time":"2017-06-19 03:00:00","inspect_at":null,"warehouse_id":4085,"task_line_name":"\u6ca7\u6d6a\u533a\u4eba\u6c11\u8def\u8425\u4e1a\u90e8","adc_id":9,"addition_comment":"","dd_id":1000438,"source_event_id":0,"car_num":"\u82cfEK301R","first_pay_money":0,"addition_seq":0,"first_check_in":0,"time_cost":18000,"bid_mgr_id":1000545,"created_at":"2017-06-07 21:12:09","comment":"","inspect_status":100,"new_trade_level_one":0,"driver_price":22500,"car_id":5,"sop_mgr_id":1000569}}}
        """.stripMargin

    test("json read") {

        println(BeepertfMessage.parseMessage(message))
    }




}
