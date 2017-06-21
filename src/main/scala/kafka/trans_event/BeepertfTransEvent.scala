package kafka.trans_event

import java.io.File
import java.util.Properties

import _root_.common._
import kafka._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SaveMode}
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

object BeepertfTransEvent extends Log with AbstractConfEnv {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("BeepertfTransEvent")

        if (conf.hasPath("beeper_trans_event.streaming.spark_master")) {
            sparkConf.setMaster(conf.getString("beeper_trans_event.streaming.spark_master"))
        }

        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        sparkConf.set("spark.sql.shuffle.partitions", "3")

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
            "heartbeat.interval.ms" -> (300 * 1000).toString,
            "session.timeout.ms" -> (1800 * 1000).toString,
            "request.timeout.ms" -> (2000 * 1000).toString
        )

        val shouldOffsetStore = conf.getBoolean("beeper_trans_event.consumer.offset_store")

        var offsetMap = Map[TopicPartition, Long]()
        if (shouldOffsetStore) {
            val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("beeper_trans_event.consumer.zookeeper"))
            offsetMap = offsetsStore.readOffsets(topic, conf.getString("beeper_trans_event.consumer.group_id"))
            .getOrElse(Map[TopicPartition, Long]())
        }

        log.info(s"kafka offset 起始位置 OffsetMap:$offsetMap")

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
            val valueRDD = rdd.map(line => {
                line.value()
            })

            if (conf.getBoolean("beeper_trans_event.streaming.save_hdfs")) {
                val hdfs = conf.getString("beeper_trans_event.hadoop.hdfs")
                val topicPath = hdfs + File.separator + topic + File.separator + DateUtil.getDay + File.separator + DateUtil.getCurrentMills
                valueRDD.saveAsTextFile(topicPath)
            }

            val lines = valueRDD.filter(line => {
                StringUtils.isNoneEmpty(line) && StringUtils.isNoneBlank(line)
            })
            .map(line => {
                line.trim()
            })
            .map(line => parseMessage(line))
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
                  |   adcId as adc_id,
                  |   timestamp as run_time,
                  |   count(distinct(if(status='400',driverId,null))) as sign_driver,
                  |   count(distinct(if(status='800',driverId,null))) as run_driver,
                  |   count(distinct(if(status='900',driverId,null))) as complete_driver,
                  |   count(distinct(if(status in ('450','500','600','950'),driverId,null))) as exception_driver,
                  |   sum(if(status = '900',eventPrice,0)) as event_price,
                  |   current_timestamp as created_at,
                  |   current_timestamp as updated_at
                  |   from view_event_data where isDel = '0' group by adcId,timestamp
                """.stripMargin

            val countDriverDataFrame = spark.sql(sql)

            // 1 hour
            lines.foreachPartition(partition => {
                val pool = RedisUtil.getPool()
                val redisConnection = pool.getResource
                partition.foreach(eventMessage => {
                    val dateTime = DateUtil.dateStr2DateTime(eventMessage.timestamp)
                    val dateTimeHour = dateTime.getHour
                    val adcId = eventMessage.adcId
                    val status = eventMessage.status
                    val driverId = eventMessage.driverId
                    // 2017-04-14_10@sign_driver ,  2017-04-14_10@run_driver , 2017-04-14_10@complete_driver  expire 1 day
                    val key = status match {
                        case "400" => dateTimeHour + "@" + adcId + "@sign_driver"
                        case "800" => dateTimeHour + "@" + adcId + "@run_driver"
                        case "900" => dateTimeHour + "@" + adcId + "@complete_driver"
                        case _ => if (Seq("450", "500", "600", "950").contains(status)) dateTimeHour + "@" + adcId + "@exception_driver" else null
                    }
                    if (StringUtils.isNotEmpty(key)) {
                        redisConnection.sadd(key, driverId)
                        redisConnection.expire(key, 3600 * 24)
                    }

                    if ("900".equals(status)) {
                        val eventPriceKey = dateTimeHour + "@" + adcId + "@event_price"
                        val eventPrice = eventMessage.eventPrice.toInt
                        redisConnection.incrBy(eventPriceKey, eventPrice)
                        redisConnection.expire(eventPriceKey, 3600 * 24)
                    }
                })
                redisConnection.close()
                RedisUtil.close(pool)
            })

            val table = "bi_stream_trans_event_ten_minute"
            val url = conf.getString("mysql.url")
            val userName = conf.getString("mysql.userName")
            val password = conf.getString("mysql.password")
//            val properties = new Properties()
//            properties.put("user", userName)
//            properties.put("password", password)
//            properties.put("driver", "com.mysql.jdbc.Driver")
//            countDriverDataFrame.write.mode(SaveMode.Append).jdbc(url, table, properties)
            val connection = DBUtil.createMySQLConnectionFactory(url, userName, password)
            val removeSql = s"delete from $table where run_time = ? and adc_id = ?"
            countDriverDataFrame.collect().foreach(row => {
                val valueMap = row.getValuesMap(countDriverDataFrame.schema.map(_.name))
                val runTime = valueMap.getOrElse("run_time","")
                val adcId = valueMap.getOrElse("adc_id","0").toInt
                val runTimeStamp = DateTime(runTime, DateTime.DATETIMEMINUTE).getDate
                DBUtil.runSQL(connection, removeSql, Seq(runTimeStamp,adcId))
                DBUtil.map2table(connection, valueMap, table)
            })


            // 读取数据
            val pool = RedisUtil.getPool()
            val redisConnection = pool.getResource
            lines.map(eventMessage => {
                val dateTime = DateUtil.dateStr2DateTime(eventMessage.timestamp)
                val dateTimeHour = dateTime.getHour
                val adcId = eventMessage.adcId
                dateTimeHour + "@" + adcId
            }).distinct()
            .collect()
            .foreach(key => {
                val signDriverKey = key + "@sign_driver"
                val runDriverKey = key + "@run_driver"
                val completeDriverKey = key + "@complete_driver"
                val exceptionDriverKey = key + "@exception_driver"
                val eventPriceKey = key + "@event_price"
                val signDriverCount = redisConnection.scard(signDriverKey)
                val runDriverCount = redisConnection.scard(runDriverKey)
                val completeDriverCount = redisConnection.scard(completeDriverKey)
                val exceptionDriverCount = redisConnection.scard(exceptionDriverKey)
                val eventPrice = redisConnection.get(eventPriceKey)

                val keyArray = key.split("@")
                val timeHour = keyArray.apply(0)
                val adcId = keyArray.apply(1).toInt
                val columnMap = Map("run_time" -> DateTime(timeHour, DateTime.DATETIMEHOUR).getDate,
                    "adc_id" -> adcId,
                    "sign_driver" -> signDriverCount,
                    "run_driver" -> runDriverCount,
                    "complete_driver" -> completeDriverCount,
                    "exception_driver" -> exceptionDriverCount,
                    "event_price" -> eventPrice,
                    "created_at" -> DateUtil.getCurrent,
                    "updated_at" -> DateUtil.getCurrent).toMap

                val deleteSQL = "delete from bi_stream_trans_event_one_hour where run_time = ? and adc_id = ?"
                DBUtil.runSQL(connection, deleteSQL, Seq(DateTime(timeHour, DateTime.DATETIMEHOUR).getDate,adcId))
                DBUtil.map2table(connection, columnMap, "bi_stream_trans_event_one_hour")

                log.info(s"时间:$timeHour 注册司机:$signDriverCount 在跑司机:$runDriverCount 完成司机:$completeDriverCount  " +
                s"异常司机:$exceptionDriverCount 基础运力费:$eventPrice")
            })
            connection.close()
            redisConnection.close()
            RedisUtil.close(pool)

            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            offsetRanges.foreach(offsetRange => {
                log.info(s"zookeeper offset: topic:${offsetRange.topic} partition:${offsetRange.partition} " +
                s"fromOffset:${offsetRange.fromOffset} endOffset:${offsetRange.untilOffset}")
            })

            val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("beeper_trans_event.consumer.zookeeper"))
            offsetsStore.saveOffsets(topic, conf.getString("beeper_trans_event.consumer.group_id"), offsetRanges)

        })

        ssc.start()
        ssc.awaitTermination()
    }

}
