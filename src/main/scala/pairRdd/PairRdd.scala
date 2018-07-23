package pairRdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}

object PairRdd {
  case class ScoreDetail(studentName: String, subject: String, score: Float)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sc = new SparkContext(conf)

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

    sc.stop()
  }
}
