import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/14.
  */
class ConfigSuite  extends FunSuite with Matchers with BeforeAndAfter {

  val conf = ConfigFactory.load("config_dev.conf")

   test("config") {

      println(conf.getString("beeper_trans_event.consumer.bootstrap_servers"))

   }

  test("exists path"){
    println(conf.hasPath("beeper_trans_event.streaming.spark_master"))
  }

}
