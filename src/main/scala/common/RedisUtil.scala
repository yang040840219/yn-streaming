package common

import com.typesafe.config.ConfigFactory
import redis.clients.jedis.{Jedis, JedisSentinelPool, HostAndPort}
import scala.collection.JavaConverters._


/**
  * Created by yxl on 17/4/15.
  */
object RedisUtil extends Log {

  val conf = ConfigFactory.load("config_dev.conf")
  val sentinelMasterName = conf.getString("redis.sentinel_master")


  def getPool(): JedisSentinelPool = {
    val sentinels = conf.getString("redis.sentinel_host").split(",").map(
      line => {
        val hostPort = line.split(":")
        new HostAndPort(hostPort(0),hostPort(1).toInt).toString
      }
    ).toSet.asJava
    val pool = new JedisSentinelPool(sentinelMasterName,sentinels)
    pool
  }

  def close(pool:JedisSentinelPool) = {
    try{
      pool.close()
    }catch{
      case ex:Exception => {
         log.error(ex)
      }
    }
  }

}
