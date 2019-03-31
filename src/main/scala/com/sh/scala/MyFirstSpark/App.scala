
package com.sh.scala.MyFirstSpark

object App {
  
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\DEVELOPMENT\\hadoop")
    val obj = new SparkOps()
    obj.writeToSpark("D:\\Mp3\\Blues")
    val total = obj.showTable().map(x=>x.getDouble(1)).sum
    println(total)
  }

}



