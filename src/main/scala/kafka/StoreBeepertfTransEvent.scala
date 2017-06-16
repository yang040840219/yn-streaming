package kafka

import java.io.File

import _root_.common.{Log, DateUtil, ZooKeeperOffsetsStore}
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager}
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
  * ps -ef | grep spark |  grep BeepertfTransEvent | awk '{print $2}'   | xargs kill  -SIGTERM
  *
  */

object StoreBeepertfTransEvent extends Log{

  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

    val conf = ConfigFactory.load("config_store.conf")

    val sparkConf = new SparkConf().setAppName("StoreBeepertfTransEvent")

    if(conf.hasPath("spark_streaming.spark_master")){
      sparkConf.setMaster(conf.getString("spark_streaming.spark_master"))
    }

    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.shuffle.partitions","5")

    val ssc = new StreamingContext(sparkConf, Seconds(conf.getInt("spark_streaming.batch_duration")))

    val topic = conf.getString("consumer.topic")

      ssc.checkpoint(conf.getString("spark_streaming.spark_checkpoint") +
            File.separator + conf.getString("spark_streaming.checkpoint_dir"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("consumer.bootstrap_servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> conf.getString("consumer.group_id"),
      "auto.offset.reset" -> conf.getString("consumer.offset_reset"),
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "heartbeat.interval.ms" -> (5 * 60 * 1000).toString,
      "session.timeout.ms" -> (5 * 60 * 1000 * 4).toString,
      "request.timeout.ms" -> (5 * 60 * 1000 * 5).toString
    )

    val shouldOffsetStore = conf.getBoolean("consumer.offset_store")

    var offsetMap = Map[TopicPartition, Long]()
    if (shouldOffsetStore) {
      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("consumer.zookeeper"))
      offsetMap = offsetsStore.readOffsets(topic,conf.getString("consumer.group_id")).getOrElse(Map[TopicPartition, Long]())
    }

    log.info(s"kafka OffsetMap:$offsetMap")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq(topic), kafkaParams, offsetMap)
    )

    stream.foreachRDD(rdd => {
      val valueRDD = rdd.map(line => { line.value()})

      if(conf.getBoolean("spark_streaming.save_hdfs")){
        val hdfs = conf.getString("hadoop.hdfs")
        val topicPath = hdfs + File.separator + topic + File.separator + DateUtil.getDay + File.separator + DateUtil.getCurrentMills
        valueRDD.saveAsTextFile(topicPath)
      }

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offsetRange => {
        log.info(s"zookeeper offset: topic:${offsetRange.topic} partition:${offsetRange.partition} " +
          s"fromOffset:${offsetRange.fromOffset} endOffset:${offsetRange.untilOffset}")
      })

      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("consumer.zookeeper"))
      offsetsStore.saveOffsets(topic,conf.getString("consumer.group_id"),offsetRanges)

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
