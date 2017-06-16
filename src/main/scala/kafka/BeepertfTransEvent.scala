package kafka

import java.io.File

import _root_.common.{Log, RedisUtil, DateUtil, ZooKeeperOffsetsStore}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.Map


/**
  * Created by yxl on 17/4/10.
  *
  * 停止：
  * ps -ef | grep spark |  grep BeepertfTransEvent | awk '{print $2}'   | xargs kill  -SIGTERM
  *
  */

object BeepertfTransEvent extends Log {



  def main(args: Array[String]) {

    val conf = ConfigFactory.load("config_dev.conf")

    val sparkConf = new SparkConf().setAppName("BeepertfTransEvent")

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

    def parseMessage(message: String): Option[TransEventMessage] = {
      try {
        BeepertfMessage.parseMessage(message)
      } catch {
        case ex: Exception => {
          log.error(ex.printStackTrace())
          log.error(s"wrong format message:$message")
          None
        }
      }
    }

    stream.foreachRDD(rdd => {
      val valueRDD = rdd.map(line => { line.value()})

      if(conf.getBoolean("spark_streaming.save_hdfs")){
        val hdfs = conf.getString("hadoop.hdfs")
        val topicPath = hdfs + File.separator + topic + File.separator + DateUtil.getDay + File.separator + DateUtil.getCurrentMills
        valueRDD.saveAsTextFile(topicPath)
      }

      val lines = valueRDD.filter(line => { StringUtils.isNoneEmpty(line) && StringUtils.isNoneBlank(line) })
        .map(line => { line.trim() })
        .map(line =>  parseMessage(line))
        .filter(line => line.nonEmpty)
        .map(line => line.get)
        .filter(line => "UPDATE".equals(line.eventType))
        .map(line => {
            line.copy(timestamp = DateUtil.getNextTenMinute(DateUtil.str2mills(line.timestamp)))
        })
        .filter(line => "0".equals(line.isDel))
        .coalesce(5)

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val eventDataFrame = lines.toDF()
      eventDataFrame.createOrReplaceTempView("view_event_data")

      // 10min 签到司机,在跑司机,配送完成司机,异常司机,基础运力价格
      val sql =
        """
          | select
          |   adcId,
          |   timestamp,
          |   count(distinct(if(status='400',driverId,null))) as sign_driver,
          |   count(distinct(if(status='800',driverId,null))) as run_driver,
          |   count(distinct(if(status='900',driverId,null))) as complete_driver,
          |   count(distinct(if(status in ('450','500','600','950'),driverId,null))) as exception_driver,
          |   sum(if(status = '900',eventPrice,0)) as event_price
          |   from view_event_data where isDel = '0' group by adcId,timestamp
        """.stripMargin

      val countDriverDataFrame = spark.sql(sql)

      countDriverDataFrame.show() // todo 写入 hbase

      // 1 hour
      lines.foreachPartition(partition => {
         val pool = RedisUtil.getPool()
         val redisConnection = pool.getResource
         partition.foreach(eventMessage => {
              val dateTime = DateUtil.dateStr2DateTime(eventMessage.timestamp)
              val dateTimeHour = dateTime.getHour
              val status = eventMessage.status
              val driverId = eventMessage.driverId
              // 2017-04-14_10@sign_driver ,  2017-04-14_10@run_driver , 2017-04-14_10@complete_driver  expire 1 day
              val key = status match {
                case "400" => dateTimeHour + "@sign_driver"
                case "800" => dateTimeHour + "@run_driver"
                case "900" => dateTimeHour + "@complete_driver"
                case _ => if(Seq("450","500","600","950").contains(status))  dateTimeHour + "@exception_driver" else null
              }
              if(StringUtils.isNotEmpty(key)){
                redisConnection.sadd(key,driverId)
                redisConnection.expire(key,3600*24)
              }

              if("900".equals(status)){
                 val eventPriceKey = dateTimeHour + "@event_price"
                 val eventPrice = eventMessage.eventPrice.toInt
                 redisConnection.incrBy(eventPriceKey,eventPrice)
                 redisConnection.expire(eventPriceKey,3600*24)
              }
         })
         redisConnection.close()
         RedisUtil.close(pool)
      })

      // 读取数据
      val pool = RedisUtil.getPool()
      val redisConnection = pool.getResource
      lines.map(eventMessage => {
        val dateTime = DateUtil.dateStr2DateTime(eventMessage.timestamp)
        val dateTimeHour = dateTime.getHour
        dateTimeHour
      }).distinct()
        .collect()
        .foreach(timeHour => {
          val signDriverKey = timeHour + "@sign_driver"
          val runDriverKey = timeHour + "@run_driver"
          val completeDriverKey = timeHour + "@complete_driver"
          val exceptionDriverKey = timeHour + "@exception_driver"
          val eventPriceKey = timeHour + "@event_price"
          val signDriverCount = redisConnection.scard(signDriverKey)
          val runDriverCount = redisConnection.scard(runDriverKey)
          val completeDriverCount = redisConnection.scard(completeDriverKey)
          val exceptionDriverCount = redisConnection.scard(exceptionDriverKey)
          val eventPrice = redisConnection.get(eventPriceKey)

          log.info(s"时间:$timeHour 注册司机:$signDriverCount 在跑司机:$runDriverCount 完成司机:$completeDriverCount  " +
            s"异常司机:$exceptionDriverCount 基础运力费:$eventPrice")
      })
      redisConnection.close()
      RedisUtil.close(pool)

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offsetRange => {
        log.info(s"zookeeper offset: topic:${offsetRange.topic} partition:${offsetRange.partition} " +
          s"fromOffset:${offsetRange.fromOffset} endOffset:${offsetRange.untilOffset}")
      })

      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("consumer.zookeeper"))
      offsetsStore.saveOffsets(topic,conf.getString("consumer.group_id"),offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
