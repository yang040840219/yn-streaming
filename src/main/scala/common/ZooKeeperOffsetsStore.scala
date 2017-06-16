package common

/**
  * Created by yxl on 17/4/13.
  */

import java.io.File

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import scala.collection.mutable._

object MyZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

class ZooKeeperOffsetsStore(zkHosts: String) extends OffsetsStore {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000,MyZKStringSerializer)

  override def readOffsets(topic: String,consumer:String): Option[Map[TopicPartition, Long]] = {
    try{
      val zkUtils = ZkUtils(zkClient,false)
      val topicPath = File.separator + topic + File.separator + consumer
      val (offsetsRangesStrOpt, _) = zkUtils.readDataMaybeNull(topicPath)
      zkUtils.close()
      offsetsRangesStrOpt match {
        case Some(offsetsRangesStr) =>
          val offsets = offsetsRangesStr.split(",")
            .map(s => s.split(":"))
            .map { case Array(partitionStr, offsetStr) => ( new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
            .toMap
          Some(Map() ++ offsets)
        case None =>
          None
      }
    }catch{
      case ex:Exception => {
        log.error(ex)
        None
      }
    }
  }

  override def saveOffsets(topic:String,consumer:String,offsetRanges:Array[OffsetRange]): Unit = {
    try{
      val zkUtils = ZkUtils(zkClient,false)
      val topicPath = File.separator + topic + File.separator + consumer
      val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}")
        .mkString(",")
      log.info(s"Writing offsets to ZooKeeper:$topic $consumer ${offsetsRangesStr}")
      zkUtils.updatePersistentPath(topicPath,offsetsRangesStr)
      zkUtils.close()
    }catch{
      case ex:Exception => {
        log.error(ex)
      }
    }
  }

}