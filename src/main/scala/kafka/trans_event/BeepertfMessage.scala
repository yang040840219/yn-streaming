package kafka.trans_event

import _root_.common.JsonUtil
import org.apache.commons.lang3.StringUtils

/**
  * Created by yxl on 17/4/14.
  */


case class TransEventMessage(msgId: String, timestamp: String, eventType: String,
                             driverId: String, adcId: String, status: String, eventPrice: String,
                             isDel: String, completeTime: String)

object BeepertfMessage {

    def isNull(value: Object, default: String = ""): String = {
        if (value == null || StringUtils.isEmpty(value.toString)) {
            default
        } else {
            value.toString
        }
    }

    def parseMessage(message: String): Option[TransEventMessage] = {
        val map = JsonUtil.toMap[Object](message)
        val data = map.get("data")
        val msgId = map.getOrElse("msgid", "0").toString
        val record = data match {
            case None => None
            case Some(eventMap) => {
                val eventDataMap = eventMap.asInstanceOf[Map[String, Object]]
                if ("yn_trans_event".equals(eventDataMap.getOrElse("table", "-"))) {
                    val timestamp = eventDataMap.getOrElse("timestamp", "0").toString
                    val eventType = eventDataMap.getOrElse("type", "-").toString
                    val values = eventDataMap.get("values")
                    values match {
                        case None => None
                        case Some(valueMap) => {
                            val eventValueMap = valueMap.asInstanceOf[Map[String, Object]]
                            val driverId = isNull(eventValueMap.getOrElse("driver_id", "0"))
                            val adcId = isNull(eventValueMap.getOrElse("adc_id", "0"))
                            val status = isNull(eventValueMap.getOrElse("status", "0"))
                            val eventPrice = isNull(eventValueMap.getOrElse("customer_price", "0"))
                            val isDel = isNull(eventValueMap.getOrElse("is_del", "0"))
                            val completeTime = isNull(eventValueMap.getOrElse("complete_time", "0"), "0")
                            Some(TransEventMessage(msgId, timestamp, eventType, driverId, adcId, status, eventPrice, isDel, completeTime.toString))
                        }
                    }
                } else {
                    None
                }
            }
        }
        record
    }

}
