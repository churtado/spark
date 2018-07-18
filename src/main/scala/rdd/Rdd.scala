package rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// https://spark.apache.org/docs/2.3.1/

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
    // examples of an actions:
    println("first row: "+ iris.first())
    println("here's 3 rows: ")
    println(iris.take(3).foreach(println))
    println("unfiltered input had " + iris.count() + " lines")
    println("filtered input had " + irisFiltered.count() + " lines")

    // this action would load the entire RDD in memory. Works if you have enough memory
    iris.collect()

    // RDD's are recomputed every time you run an action on them but you can reuse them by persisting them in memory
    iris.persist()
    iris.first()
    iris.count()

    // use collect() to get the entire dataset: it must fit in memory
    iris.collect().foreach(println)

    // workflow: create rdd, transform, persist, query, and save
    // stop
    sc.stop()
  }
}

class SearchFunctions(val query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }
  /*def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    rdd.map(isMatch)
  }*/
}
