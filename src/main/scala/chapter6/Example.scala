package chapter6

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner}

object Example {
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
      if(line.length() == 0) blankLines.add(1)
    }

    //callSigns.saveAsTextFile("output.txt")
    println(s"Blank lines: $blankLines")

  }
}
