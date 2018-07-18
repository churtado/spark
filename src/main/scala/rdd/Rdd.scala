package rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.avro.generic.GenericRecord
import org.apache.spark.storage.StorageLevel

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
    println(iris.take(3).foreach(println)) // foreach lets you run computations on an rdd without bringing it back locally
    println("unfiltered input had " + iris.count() + " lines")
    println("filtered input had " + irisFiltered.count() + " lines")

    // this action would load the entire RDD in memory. Works if you have enough memory
    iris.collect()

    // RDD's are recomputed every time you run an action on them but you can reuse them by persisting them in memory
    iris.persist()
    iris.first()
    iris.top(3) // top elements
    iris.count()
    iris.take(3)
    iris.takeSample(withReplacement = true, 5, 1 )

    // some examples with map
    val nums = sc.parallelize(List(1,1,2,3,4))
    val squared = nums.map(x => x * x)
    println("Example with map transformation")
    println(squared.collect().mkString(","))

    // flatMap is called for every element of the input RDD, and you get back an RDD of iterators for every element
    // that is "flattened" to one iterator for all the resulting elements. See below:
    val lines2 = sc.parallelize(List("hello world", "hi"))
    val words2 = lines2.flatMap(line => line.split(" "))
    println("flatmap example")
    println(words2.collect().mkString(" , "))

    // applying set operations
    println("distinct elements in a set")
    println(nums.distinct().collect().mkString(" , "))

    println("union of 2 sets")
    val temp2 = words2.union(lines)
    println(temp2.collect().mkString(","))

    println("intersection")
    val temp3 = words2.intersection(lines)
    println(temp3.collect().mkString(","))

    println("A - B")
    val temp4 = words2.subtract(lines)
    println(temp4.collect().mkString(","))

    println("Cartesian")
    println("Cartesian")
    val temp5 = words2.cartesian(lines)
    println(temp5.collect().mkString(","))

    val countV = words2.countByValue() // counts of each element by value

    // reduce action is the most commonly used:
    val sum = nums.reduce((x, y) => x + y)

    // fold is reduce but with an initial accumulator parameter
    val sumFold = nums.fold(zeroValue = 0)((x, y) => x + y)

    // both reduce and fold require returning same type as input. Not so with aggregate:
    // this is a running sum; notice the similarity in map-and-reduce style operations
    val result = nums.aggregate(0, 0)((acc, value) => (acc._1 + value, acc._2 + 1), // combine both elements with accumulator
                                      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) // combine the tuples (<number>, 1)
    val avg = result._1 / result._2.toDouble

    // use collect() to get the entire dataset: it must fit in memory
    // iris.collect().foreach(println)

    // sometimes you need to convert to a certain type of RDD to do certain operations. In Scala it's implicit, but be careful

    // Persistence: handle it by levels!!!
    //nums.persist(StorageLevel.MEMORY_AND_DISK)

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
