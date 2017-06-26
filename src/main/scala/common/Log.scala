package common

import org.apache.commons.logging.LogFactory

/**
  * Created by yxl on 17/5/9.
  */
trait Log {
  @transient val log =  LogFactory.getLog("console")
}
