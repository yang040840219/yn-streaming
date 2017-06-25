package common

import java.sql._
import java.util.{Date, Properties}

import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper}

import scala.collection.JavaConverters._

/**
  * Created by yxl on 17/4/12.
  */
object DBUtil extends Serializable with Log {

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

    def createMySQLConnectionFactory(url: String, userName: String, password: String): Connection = {
        val properties = new Properties()
        properties.setProperty("user", userName)
        properties.setProperty("password", password)
        properties.setProperty("useUnicode", "true")
        properties.setProperty("characterEncoding", "UTF-8")
        properties.setProperty("driver", "com.mysql.jdbc.Driver")
        createMySQLConnectionFactory(url, properties)
    }


    def insertTable(connection: Connection, map: Map[String, Any], tableName: String): Int = {
        val columns = map.filter( x => x.productElement(1) != null).keys.toSeq
        val values = columns.map(column => "?")
        val sqlBuffer = new StringBuffer()
        sqlBuffer.append("insert into ")
        .append(tableName)
        .append(columns.map(column => "`" + column + "`").mkString("(", ",", ")"))
        .append(" values").append(values.mkString("(", ",", ")"))
        val sql = sqlBuffer.toString
        val statement = connection.prepareStatement(sql)
        columns.zip(Stream.from(1)).map(item => {
            item match {
                case (column: String, index: Int) => {
                    val columnValue = map.get(column)
                    setStatementParam(statement, index, columnValue.get)
                }
                case _ => throw new Exception(s"列名格式 $columns")
            }
        })
        statement.executeUpdate()
    }

    def setStatementParam(statement: PreparedStatement, index: Int, columnValue: Any): Unit = {
        columnValue match {
            case v: String => statement.setString(index, v)
            case v: Integer => statement.setInt(index, v.toString.toInt)
            case v: Double => statement.setDouble(index, v.toString.toDouble)
            case v: Long => statement.setLong(index, v.toString.toLong)
            case v: Date =>
                {   val timestamp =  new Timestamp(v.getTime)
                    statement.setTimestamp(index, timestamp) }
            case v: Float => statement.setFloat(index, v.toString.toFloat)
            case _ => throw new Exception(s"未知类型 $columnValue")
                // 没有判断 null 的情况, 在 map2table 中 过滤
        }

    }

    def runSQL(connection: Connection, sql: String, param : Seq[Any]): Int = {
        val statement = connection.prepareStatement(sql)
        param.zip(Stream.from(1)).map(item => {
            item match {
                case (column: Any , index: Int) => {
                    setStatementParam(statement, index,column)
                }
                case _ => throw new Exception(s"列名格式 $param")
            }
        }
        )
        log.info(s"运行 SQL:$sql 参数: ${param.mkString(",")}")
        statement.executeUpdate()
    }


    def runQuerySQLCount(connection: Connection, sql: String, param : Seq[Any]): Int = {
        val statement = connection.prepareStatement(sql)
        param.zip(Stream.from(1)).map(item => {
            item match {
                case (column: Any , index: Int) => {
                    setStatementParam(statement, index,column)
                }
                case _ => throw new Exception(s"列名格式 $param")
            }
        }
        )
        log.info(s"运行 SQL:$sql 参数: ${param.mkString(",")}")
        val resultSet = statement.executeQuery()
        resultSet.last()
        resultSet.getRow()
    }


    def resultSet2Seq(resultSet: ResultSet): Option[Seq[Map[String, AnyRef]]] = {
        val md = resultSet.getMetaData
        val colNames = for (i <- 1 to md.getColumnCount) yield md.getColumnName(i)
        resultSet.last()
        val rowCount = resultSet.getRow()
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
