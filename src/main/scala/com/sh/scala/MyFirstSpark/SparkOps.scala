package com.sh.scala.MyFirstSpark

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType



case class Fichero(f: File, tama: BigDecimal)

class SparkOps {
   val rutaDestino = "C:\\DEVELOPMENT\\hadoop\\data"

  val conf = new SparkConf()
    .setAppName("Read Folder")
    .setMaster("local[*]")
    .set("spark.sql.catalogImplementation", "hive")
    .set("hive.metastore.warehouse.dir", "'file:/D:/Desarrollo/scalaWorkspace/MyFirstSpark/spark-warehouse/")
  val sc = new SparkContext(conf)
  val sqlCtx = new SQLContext(sc)

  def writeToSpark(ruta: String): Unit = {
    val res = getFiles(ruta)
    val schema = StructType(Array(StructField("File", StringType, true), StructField("Size", DoubleType, true)))
    val rowRDD = sc.parallelize(res, 1).map(v => Row(v.f.getAbsolutePath, v.tama.doubleValue()))
    val df = sqlCtx.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("dirTemp")
    write2Hive()  
  }
  
  def write2Hive() = {
    sqlCtx.sql("drop table if exists dirtbl")
    sqlCtx.sql("create table dirtbl as select * from dirTemp")
    
  }
  def showTable(): Array[Row] =  {
    
    sqlCtx.sql("select * from dirtbl").collect()
    //allrecords.show()
  }

  def readDir(dir: String): Array[File] = {
    val f = new File(dir)
    val ls = f.listFiles()
    ls ++ ls.filter(_.isDirectory).flatMap(it => readDir(it.getAbsolutePath))

  }

  def calcular(f: File): Fichero = {

    val path = Paths.get(f.getAbsolutePath)
    val tam: Double = BigDecimal((FileChannel.open(path).size() / 1024F) / 1024F).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    new Fichero(f, tam)
  }

  def writeTotxt(col: Array[Fichero]) = {
    val path = Paths.get(s"$rutaDestino\\input.txt")
    if (Files.exists(path)) {
      Files.delete(path)
    }
    col.map(x => s"${x.f.getAbsolutePath}##${x.tama.toString()} MB\n")
      .foreach(f => {
        if (Files.exists(path)) {
          Files.write(path, f.getBytes, StandardOpenOption.APPEND)
        } else {
          Files.write(path, f.getBytes, StandardOpenOption.CREATE)
        }
      })
  }

  def getFiles(ruta: String): Array[Fichero] = {
    readDir(ruta).filter(_.isFile).map(calcular(_))
  }
}