package common

import java.util.UUID

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by yxl on 15/9/6.
 */
object RDDExtend {

  implicit class RddOp[P <: Any](val rdd: RDD[P]) {

    def saveAsMergedTextFile(path: String, overwrite: Boolean = true): Unit = {
      if (overwrite) {
        delete(rdd.context, path)
      }
      val dstPath = new Path(path)
      val tmpDir = dstPath.getParent().toString + UUID.randomUUID()
      rdd.saveAsTextFile(tmpDir)
      merge(rdd.context, tmpDir, path)
    }


    /**
     * 合并输出的文件，可以做成 implicit的做为RDD的一个方法
     * @param sc
     * @param srcDir
     * @param dstDir
     */
    def merge(sc: SparkContext, srcDir: String,
              dstDir: String): Unit = {
      val srcPath = new Path(srcDir)
      val dstPath = new Path(dstDir + "/part-00000")
      val srcFileSystem = srcPath.getFileSystem(sc.hadoopConfiguration)
      val dstFileSystem = dstPath.getFileSystem(sc.hadoopConfiguration)
      FileUtil.copyMerge(srcFileSystem, srcPath,
        dstFileSystem, dstPath,
        true, sc.hadoopConfiguration, null)
      dstFileSystem.delete(srcPath, true) // 删除临时目录
    }

    /**
     * 删除目录/文件
     * @param sc
     * @param dir
     */
    def delete(sc: SparkContext, dir: String): Unit = {
      val dirPath = new Path(dir)
      val fs = dirPath.getFileSystem(sc.hadoopConfiguration)
      fs.delete(dirPath, true)
    }

  }

}
