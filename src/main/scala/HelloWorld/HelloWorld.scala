package HelloWorld

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloWorld {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sc = new SparkContext(conf)

    println("Hello, World!")

    sc.stop()
  }
}
