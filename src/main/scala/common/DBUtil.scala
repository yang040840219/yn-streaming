package common

import java.sql.{ResultSet, Driver, DriverManager, Connection}
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.execution.datasources.jdbc.{DriverWrapper, DriverRegistry}

/**
  * Created by yxl on 17/4/12.
  */
object DBUtil extends Serializable {

  def createMySQLConnectionFactory(url: String, properties: Properties): Connection = {
    val userSpecifiedDriverClass = Option(properties.getProperty("driver"))
    userSpecifiedDriverClass.foreach(DriverRegistry.register)
    val driverClass: String = userSpecifiedDriverClass.getOrElse {
      DriverManager.getDriver(url).getClass.getCanonicalName
    }
    DriverRegistry.register(driverClass)
    val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
      case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
      case d if d.getClass.getCanonicalName == driverClass => d
    }.getOrElse {
      throw new IllegalStateException(
        s"Did not find registered driver with class $driverClass")
    }
    driver.connect(url, properties)
  }

  def createMySQLConnectionFactory(url:String,userName:String,password:String):Connection = {
    val properties = new Properties()
    properties.setProperty("user", userName)
    properties.setProperty("password", password)
    properties.setProperty("useUnicode", "true")
    properties.setProperty("characterEncoding", "UTF-8")
    properties.setProperty("driver","com.mysql.cj.jdbc.Driver")
    createMySQLConnectionFactory(url,properties)
  }

  def resultSet2Seq(resultSet:ResultSet): Option[Seq[Map[String,AnyRef]]] = {
    val md = resultSet.getMetaData
    val colNames = for (i <- 1 to md.getColumnCount) yield md.getColumnName(i)
    resultSet.last()
    val rowCount =  resultSet.getRow()
    rowCount match {
      case 0 => None
      case _ => {
        resultSet.beforeFirst()
        val rows =
          for {
            i <- 1 to rowCount
          } yield {
            resultSet.next()
            val results = colNames map (n => resultSet.getObject(n))
            Map(colNames zip results: _*)
          }
        Some(rows)
      }
    }
  }
}
