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

    // some examples with map
    val nums = sc.parallelize(List(1,2,3,4))
    val squared = nums.map(x => x * x)
    println("Example with map transformation")
    println(squared.collect().mkString(","))

    // flatMap is called for every element of the input RDD, and you get back an RDD of iterators for every element
    val lines2 = sc.parallelize(List("hello world", "hi"))
    val words2 = lines2.flatMap(line => line.split(" "))
    println("flatmap example")
    println(words2.first())


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
  // pass references to avoid serializing the whole object and passing shit you don't need
  def getMatchesFunctionReference(rdd: RDD[String]): RDD[Boolean] = {
    rdd.map(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]) = {
    rdd.map(x => x.split(query))
  }

  def getMatchesNoReference(rdd: RDD[String]):RDD[Array[String]] = {
    // Safe: extract just the field we need into a local variable
    val query_ = this.query
    rdd.map(x => x.split(query_))
  }
}
