package pairRdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner}

object PairRdd {
  case class ScoreDetail(studentName: String, subject: String, score: Float)
  case class Store(name: String)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    //val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()


    val sc = spark.sparkContext // doing it this way to be able to import implicits

    import spark.implicits._

    spark.conf.set("spark.sql.crossJoin.enabled", "true") // to enable cartesian products

    // we've created a small rdd
    val lines = sc.parallelize(List("panda 0", "pink 3", "pirate 3", "panda 1", "pink 4"))

    // creating a pair RDD
    val pairs = lines.map{ x =>
      (x.split(" ")(0), (x.split(" ")(1).toInt))
    }

    print("pairs: ")
    println(pairs.collect().mkString(","))

    // same operations as rdd's are possible

    // filter on a pair RDD
    val result = pairs.filter{case (key, value) => value < 4}
    print("filtered pairs: ")
    println(result.collect().mkString(","))

    // rolling averages on tuples
    val reduceResult = pairs.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    // example: for panda: 7 / 2 is the average for the key "panda"
    // and its values (1 being used as a tuple counter, so just divide one by the other:
    println(reduceResult.collect().mkString(","))

    // word count using pair rdd's
    val shakespeare = sc.textFile("d:/data/shakespeare.txt")
    val words = shakespeare.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)

    //println(words.collect().mkString(","))

    val scores = List(
      ScoreDetail("A", "Math", 98),
      ScoreDetail("A", "English", 88),
      ScoreDetail("B", "Math", 75),
      ScoreDetail("B", "English", 78),
      ScoreDetail("C", "Math", 90),
      ScoreDetail("C", "English", 80),
      ScoreDetail("D", "Math", 91),
      ScoreDetail("D", "English", 80)
    )

    val scoresWithKey = for { i <- scores } yield (i.studentName, i)

    // specifying 3 partitions for my RDD to work in parallel in my cluster
    // most of the operators accept a second param = # partitions
    val scoresWithKeyRDD = sc.parallelize(scoresWithKey).partitionBy(new HashPartitioner(3)).cache

    println("\npartition lengths and items")
    scoresWithKeyRDD.foreachPartition(partition => println(partition.length))
    scoresWithKeyRDD.foreachPartition(partition => println(partition.foreach(item => println(item._2))))

    // combineByKey is similar to reduce. It works per partition, then merges all the partitions in the rdd
    // add's keys as it finds them. It creates accumulators on the fly via the createCombiner() function
    // and you pass it an agg function
    val avgScoresRDD = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1), // mapping each item as a key with its count, aka createCombiner (key, value) -> (key, (value, 1))

      // merge values of all the partitions: (key,(value, 1)) -> (key, (total, count))
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1), // merge values
      // total across all partitions: (key, (total, count)) -> (key, (totalAllPartitions, countAllPartitions))
      (acc1: (Float, Int), acc2:(Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // merge combiners
    ).map( {case(key, value) => (key, value._1/value._2)})

    println("\ncombine by key:")
    avgScoresRDD.collect.foreach(println)

    val reduceData = Seq(("a", 3), ("b", 4), ("a", 1))
    sc.parallelize(reduceData).reduceByKey((x,y) => x + y) // default parallelism
    sc.parallelize(reduceData).reduceByKey((x,y) => x + y, 10) // choosing 10 partitions

    val partitionExample = spark.sparkContext.parallelize(reduceData).reduceByKey((x,y) => x + y) // default parallelism
    partitionExample.repartition(3) // shuffle across the network, very expensive operation
    // faster than repartition, avoids data movement but only if decreasing partitions
    partitionExample.coalesce(3)

    // make sure you can safely call coalesce by checking the size of partitions to make sure it's reducing
    println(partitionExample.partitions.size)

    // groupByKey : (key, value) -> [key, iterable(values)]
    // the byKey operations we've seen can be more efficient because they create aggregated rdd's
    // meanwhile the the group by creates a list, that's less efficient

    // cogroup() mixes data from multiple rdd's with the same key type: -> [key, iterable(v1), iterable(v2)]
    // cogroup is used for joins

    println("##########################")

    // uses implicits
    val employees = sc.parallelize(Array[(String, Option[Int])](
      ("Rafferty", Some(31)), ("Jones", Some(33)), ("Heisenberg", Some(33)), ("Robinson", Some(34)), ("Smith", Some(34)), ("Williams", null)
    )).toDF("LastName", "DepartmentID")

    employees.show()

    val departments = sc.parallelize(Array(
      (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
      (35, "Marketing")
    )).toDF("DepartmentID", "DepartmentName")

    departments.show()

    employees
      .join(departments, "DepartmentID")
      .show()

    employees
      .join(departments, Seq("DepartmentID"), "left_outer")
      .show()

    employees
      .join(departments)  // cartesian product
      .show(10)

    val departmentsSortRDD = sc.parallelize(Array(
      (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
      (35, "Marketing")
    )).sortByKey()

    println("######################## sorting ########################")
    departmentsSortRDD.collect.foreach(println)


    // controlling partitioning explicitly is possible in spark
    // only useful in cases like doing joins
    // works with key/value pairs, ensure a set of keys appear on a certain node
    // basically you build your own hash for partitioning:
    // .partitionBy(new HashPartitioner(100)) // Create 100 partitions

    // partitionBy is a transformation, returns new RDD, meanwhile all other processing remains unchanged
    // additionally, calls to join will "know" about the hash to reduce network traffic
    // so join(A) means only A gets shuffled around the network, and to partitions that contain the hash
    // being joined on

    // these transforms result in known number of paritions, and joins will use this info:
    // sortByKey and groupByKey -> range partitioned and hash-partitioned RDD's respectively
    // find out the partitioner with the .partitioner property

    // operations that act on one rdd involve shuffling in the network: reduceByKey, etc.
    // binary operations will cause at least one rdd to be shuffled
    // if both rdd's have same partitioner and cached on the same machiens, no shuffling occurs (p. 65)


    spark.stop()
    //sc.stop()
  }
}
