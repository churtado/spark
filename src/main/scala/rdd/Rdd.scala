package rdd

import org.apache.spark.{SparkConf, SparkContext}

object Rdd {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sc = new SparkContext(conf)

    // we've created a small rdd
    val lines = sc.parallelize(List("pandas", "i like pandas"))

    // we've created an rdd by loading from external storage
    val iris  = sc.textFile("d:/data/iris.csv")

    // 2 types of operations transformations and actions
    // transformations are lazy, only used in actions

    // returns pointer to a new rdd
    val irisFiltered = iris.filter(line => line.contains("1"))

    // spark keeps track of transformations with a lineage graph to see all the RDD's daddies

    // actions return values, and make spark do the work
    // example of an action:
    println("unfiltered input had " + iris.count() + " lines")
    println("filtered input had " + irisFiltered.count() + " lines")

    // this action would load the entire RDD in memory. Works if you have enough memory
    iris.collect();

    sc.stop()
  }
}
