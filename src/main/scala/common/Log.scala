package common

import org.apache.log4j.{Level, LogManager}

/**
  * Created by yxl on 17/5/9.
  */
trait Log {
  @transient val log = LogManager.getRootLogger
  log.setLevel(Level.ERROR)
}
