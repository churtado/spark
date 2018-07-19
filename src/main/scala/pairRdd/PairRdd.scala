package pairRdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.avro.generic.GenericRecord
import org.apache.spark.storage.StorageLevel

object PairRdd {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sc = new SparkContext(conf)

    // we've created a small rdd
    val lines = sc.parallelize(List("1 pandas", "2 foxes"))

    // creating a pair RDD
    val pairs = lines.map(x => (x.split(" ")(0), x.split(" ")(1)))

    println(pairs.collect().mkString(","))

    // same operations as rdd's are possible

    // filter on a pair RDD
    val result = pairs.filter{case (key, value) => value.length < 6}

    println(result.collect().mkString(","))

    sc.stop()
  }
}
