package kafka

import com.typesafe.config.ConfigFactory

/**
  * Created by yxl on 17/6/19.
  */


trait EnvState

case object BeeperTransEventProductEnvState extends EnvState
case object BeeperTransEventDevelopEnvState extends EnvState
case object StoreBeeperTransEventEnvState extends EnvState


trait ConfEnv {

    /**
      * 用来设置读取配置文件
      * @return
      */
    def getEnv:EnvState

    val conf =
        getEnv match {
            case BeeperTransEventProductEnvState =>  ConfigFactory.load("config_pro.conf")
            case BeeperTransEventDevelopEnvState =>  ConfigFactory.load("config_dev.conf")
            case StoreBeeperTransEventEnvState =>  ConfigFactory.load("config_store.conf")
            case _ => throw new Exception("EnvState not match")
        }
}

trait AbstractConfEnv extends ConfEnv {
    /**
      * 用来设置读取配置文件
      * @return
      */
    override def getEnv: EnvState = BeeperTransEventProductEnvState
}