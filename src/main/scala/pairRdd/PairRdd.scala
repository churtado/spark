package pairRdd

import org.apache.spark.{SparkConf, SparkContext}

object PairRdd {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sc = new SparkContext(conf)

    // we've created a small rdd
    val lines = sc.parallelize(List("panda 0", "pink 3", "pirate 3", "panda 1", "pink 4"))

    // creating a pair RDD
    val pairs = lines.map{ x =>
      val str = x.split(" ")
      (str(0), str(1))
    }

    println(pairs.collect().mkString(","))

    // same operations as rdd's are possible

    // filter on a pair RDD
    val result = pairs.filter{case (key, value) => value.toInt < 4}
    println(result.collect().mkString(","))

    // rolling averages on tuples


    sc.stop()
  }
}
