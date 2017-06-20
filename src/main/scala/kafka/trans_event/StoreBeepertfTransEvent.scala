package kafka.trans_event

import java.io.File

import _root_.common.{DateUtil, Log, ZooKeeperOffsetsStore}
import com.typesafe.config.ConfigFactory
import kafka.AbstractConfEnv
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable.Map


/**
  * Created by yxl on 17/4/10.
  *
  * 停止：
  * ps -ef | grep spark |  grep StoreBeepertfTransEvent | awk '{print $2}'   | xargs kill  -SIGTERM
  *
  */

object StoreBeepertfTransEvent extends Log with AbstractConfEnv {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StoreBeepertfTransEvent")

    if(conf.hasPath("beeper_trans_event.streaming.spark_master")){
      sparkConf.setMaster(conf.getString("beeper_trans_event.streaming.spark_master"))
    }

    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.shuffle.partitions","5")

    val ssc = new StreamingContext(sparkConf, Seconds(conf.getInt("beeper_trans_event.streaming.batch_duration")))

    val topic = conf.getString("beeper_trans_event.consumer.topic")

      ssc.checkpoint(conf.getString("beeper_trans_event.streaming.spark_checkpoint") +
            File.separator + conf.getString("beeper_trans_event.streaming.checkpoint_dir"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("beeper_trans_event.consumer.bootstrap_servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> conf.getString("beeper_trans_event.consumer.group_id"),
      "auto.offset.reset" -> conf.getString("beeper_trans_event.consumer.offset_reset"),
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "heartbeat.interval.ms" -> (5 * 60 * 1000).toString,
      "session.timeout.ms" -> (5 * 60 * 1000 * 4).toString,
      "request.timeout.ms" -> (5 * 60 * 1000 * 5).toString
    )

    val shouldOffsetStore = conf.getBoolean("beeper_trans_event.consumer.offset_store")

    var offsetMap = Map[TopicPartition, Long]()
    if (shouldOffsetStore) {
      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("beeper_trans_event.consumer.zookeeper"))
      offsetMap = offsetsStore.readOffsets(topic,conf.getString("beeper_trans_event.consumer.group_id")).getOrElse(Map[TopicPartition, Long]())
    }

    log.info(s"kafka OffsetMap:$offsetMap")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq(topic), kafkaParams, offsetMap)
    )

    stream.foreachRDD(rdd => {
      val valueRDD = rdd.map(line => { line.value()})

      if(conf.getBoolean("beeper_trans_event.streaming.save_hdfs")){
        val hdfs = conf.getString("beeper_trans_event.hadoop.hdfs")
        val topicPath = hdfs + File.separator + topic + File.separator + DateUtil.getDay + File.separator + DateUtil.getCurrentMills
        valueRDD.saveAsTextFile(topicPath)
      }

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offsetRange => {
        log.info(s"zookeeper offset: topic:${offsetRange.topic} partition:${offsetRange.partition} " +
          s"fromOffset:${offsetRange.fromOffset} endOffset:${offsetRange.untilOffset}")
      })

      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("beeper_trans_event.consumer.zookeeper"))
      offsetsStore.saveOffsets(topic,conf.getString("beeper_trans_event.consumer.group_id"),offsetRanges)

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
