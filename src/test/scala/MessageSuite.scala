import kafka.BeepertfMessage
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/14.
  */
class MessageSuite  extends FunSuite with Matchers with BeforeAndAfter {

  val message =
    """
      |{"msgid":2888536571,"data":{"event_type":30,"database":"beeper_tf","timestamp":"2017-04-14 18:15:47","storage":"mysql","values":{"comment":"","check_in_latitude":0,"task_work_end_time":"1900-01-01 12:12:00","customer_tcsp":0,"task_work_begin_time":"1900-01-01 04:04:00","new_trade_level_one":0,"is_del":0,"task_is_back":1,"updated_at":"2017-04-14 18:06:47","fcc_id":0,"trans_task_id":1515870,"departure_latitude":0,"driver_id":2335154,"is_late":0,"sales_id":1001294,"dprice_subsidy_total":0,"sop_price_tax":0,"first_onboard_price":0,"id":5166636,"task_type":200,"check_in_ip":"","complete_time":null,"cprice_per_day_tax":0,"first_pay_money":0,"departure_ip":"","car_team_id":0,"cargo_insurance_price":0,"departure_longitude":0,"invoice_contents":"","dprice_per_day":22000,"task_warehouse_id":33640,"first_onboard_rate":0,"customer_id":12137,"type":100,"cprice_total":0,"check_in_longitude":0,"task_onboard_date":"2017-04-15 00:12:00","status":100,"is_wrong_location":0,"cargo_insurance_rate":0,"invoice_tax_rate":0,"is_departure":0,"addition_count":0,"work_time":"2017-05-02 04:04:00","inspect_at":null,"warehouse_id":33640,"task_line_name":"\u4e30\u53f0\u00b7\u77f3\u666f\u5c71","is_addition":0,"addition_comment":"","sop_rate":0,"dd_id":1000007,"trans_driver_bid_id":3740009,"sop_royalty_price":0,"source_event_id":0,"cprice_per_day":0,"car_num":"\u4eacEH9082","have_sop":1,"departure_time":null,"cprice_total_with_tax":0,"addition_seq":0,"day_off_refund_rate":0,"is_complete":0,"first_check_in":0,"sop_royalty_rate":0,"bid_mgr_id":1001404,"created_at":"2017-04-14 18:06:47","cprice_subsidy_total":0,"inspect_status":100,"cargo_insurance_price_tax":0,"exception_id":0,"check_in_time":null,"task_schedule":"FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR,SA,SU","day_off_refund_price":0,"adc_id":1,"is_supplement":0,"car_id":3,"sop_mgr_id":1001295,"have_temp_ctrl":0,"sop_price":0},"table":"trans_event","type":"INSERT"}}
    """.stripMargin

  test("json read") {

     println(BeepertfMessage.parseMessage(message))


  }


}
