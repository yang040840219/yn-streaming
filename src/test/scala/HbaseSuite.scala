import java.util.ServiceLoader

import common.HbaseUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.scheduler.ExternalClusterManager
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/18.
  */
class HbaseSuite extends FunSuite with Matchers with BeforeAndAfter {

    val conf = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(conf)

    test("print hbase conf") {
        println(conf)
    }

    test("create table") {

        val admin = connection.getAdmin()

        val table = TableName.valueOf("t_test")
        val families = Seq("cf1", "cf2")
        if (!admin.tableExists(table) && !admin.isTableAvailable(table)) {
            val tableDesc = new HTableDescriptor(table)
            families foreach { family =>
                tableDesc.addFamily(new HColumnDescriptor(family))
            }
            admin.createTable(tableDesc)
        }
        connection.close()
    }

    test("show tables") {
        val admin = connection.getAdmin
        admin.listTables().foreach(println _)
    }


    test("write value to table") {
        val cf = "cf2"
        val qualifier = "name"

        val table = connection.getTable(TableName.valueOf("t_test"))

        val put = new Put(Bytes.toBytes("1"))

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes("spark"))

        table.put(put)
    }


    test("write value with version") {

        //  alter 't_test', {NAME => 'cf1', VERSIONS => 100}

        val cf = "cf2"
        val qualifier = "name"
        val table = connection.getTable(TableName.valueOf("t_test"))
        val put = new Put(Bytes.toBytes("1"))

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), 1, Bytes.toBytes("spark1"))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), 2, Bytes.toBytes("spark2"))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), 3, Bytes.toBytes("spark3"))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), 4, Bytes.toBytes("spark4"))

        table.put(put)

    }

    test("get table row 1") {


        val table = connection.getTable(TableName.valueOf("t_test"))

        val get = new Get(Bytes.toBytes("1"))

        val result = table.get(get)

        val cells = result.rawCells()

        cells.foreach(cell => {
            println(Bytes.toString(CellUtil.cloneFamily(cell)))
        })

    }

    test("scan table") {

        val table = connection.getTable(TableName.valueOf("KYLIN_ZOLT10Z3Q2"))

        val scan = new Scan()

        scan.addFamily(Bytes.toBytes("F1"))
        scan.setMaxVersions(10)
        scan.setMaxResultSize(512)

        val result = table.getScanner(scan)

        val it = result.iterator()

        while (it.hasNext) {
            val row = it.next()
            row.rawCells().foreach(cell => {
                val row = Bytes.toString(CellUtil.cloneRow(cell))
                val cf = Bytes.toString(CellUtil.cloneFamily(cell))
                val qf = Bytes.toString(CellUtil.cloneQualifier(cell))
                val value = Bytes.toString(CellUtil.cloneValue(cell))
                val timestamp = cell.getTimestamp()
                println(s"$row , $cf , $qf  , $timestamp , $value")
            })
        }

    }

    test("delete value default by version") {

        val table = connection.getTable(TableName.valueOf("t_test"))

        val row = Bytes.toBytes("1")
        val delete = new Delete(row)
        val cf = Bytes.toBytes("cf2")
        val qualifier = Bytes.toBytes("name")
        delete.addColumn(cf, qualifier)

        table.delete(delete)

    }

    test("remove table"){
        val hbaseUtil = HbaseUtil()
        val tables = Seq(
            "kylin2_metadata",
            "kylin2_metadata_acl",
            "kylin2_metadata_user",
            "kylin_metadata",
            "kylin_metadata_acl",
            "kylin_metadata_user"
        )
        tables.foreach(x => hbaseUtil.dropTable(x))

    }

    test("hbase util connection") {

        val hbaseUtil = HbaseUtil()

        hbaseUtil.createTable("t_test_1", Seq[String]("event", "customer", "driver"))

        hbaseUtil.closeConnection

    }

}
