import common.{MyZKStringSerializer, ZooKeeperOffsetsStore}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka010.{OffsetRange, KafkaUtils}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/13.
  */
class ZookeeperSuite extends FunSuite with Matchers with BeforeAndAfter {

  val topic = "test-write-read-offset"

  val consumer = "consumer_2"

  val  zookeeperStore = new ZooKeeperOffsetsStore("localhost:2181")

  test("save offset") {

    val offsetRange = OffsetRange(topic,0,1,11)

    zookeeperStore.saveOffsets(topic,consumer,Seq(offsetRange).toArray)

  }

  test("read offset"){

    println(zookeeperStore.readOffsets(topic,consumer))

  }

  test("delete path") {

    val zkClient = new ZkClient("localhost:2181", 10000, 10000,MyZKStringSerializer)

    val zkUtil =  ZkUtils(zkClient,false)

    Seq("brokers/topics/binlog_beeper_tf_trans_event").map(x => {
      zkUtil.deletePathRecursive("/" + x)
    })


  }

}
