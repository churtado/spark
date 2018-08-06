package chapter6

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner}

object Example1 {
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

    val file = sc.textFile("D:/data/callsigns.txt")

    /**
      * We'll be looking at shared variables. These variables are shared between partitions/nodes
      * updates to their values as well. They're not just local copies. There are 2 types:
      * accumulators and broadcast variables
      */

    // let's look at an accumulator for counting blank lines in the callsigns file

    // create an accumulator initialized to 0
    val blankLines = sc.longAccumulator("counter")

    val callSigns = file.foreach { line =>
      if(line.length() == 0) blankLines.add(1) // worker code in spark closures do this
    }

    /**
      * worker nodes can't access accumulator's values, this is done for efficiency
      * so that the value doesn't have to be updated for a worker (write-only)
      * good for when you need to trace several values
      *
      * Warning: accumulators in transforms aren't necessarily updated, because trans-
      * formations are lazy. It may also happen that accumulators are re-calculated if
      * they belong to a cached rdd that was evicted. So, re-calculate rdd, apply trans-
      * forms, and recalculate accumulators. This may change in the future.
      *
      * Note: you can also create custom accumulators
      */
    //callSigns.saveAsTextFile("output.txt")
    println(s"Blank lines: $blankLines")

  }
}
