package chapter6

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession


object BroadcastVariables {
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

    val callSignRegex = "\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\Z".r

    val inputFile = "D:/data/callsigns.txt"

    val validSignCount = sc.longAccumulator("valid")
    val invalidSignCount = sc.longAccumulator("invalid")
    val errorLines = sc.longAccumulator("errors")
    val dataLines = sc.longAccumulator("data")

    val file = sc.textFile(inputFile)

    val callSigns = file.flatMap(line => {
      if (line == "") {
        errorLines.add(1)
      } else {
        dataLines.add(1)
      }
      line.split(" ")
    })

    val validSigns = callSigns.filter{sign =>
      if ((callSignRegex findFirstIn sign).nonEmpty) {
        validSignCount.add(1); true
      } else {
        invalidSignCount.add(1); false
      }
    }

    val contactCounts = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)
    // Force evaluation so the counters are populated
    contactCounts.count()
    if (invalidSignCount.value < 0.5 * validSignCount.value) {
      //contactCounts.saveAsTextFile(outputDir + "/output.txt")
    } else {
      println(s"Too many errors ${invalidSignCount.value} for ${validSignCount.value}")
      //exit(1)
    }


    /**
      * Broadcast variables are the other side of the coin
      * Send a read-only variable to all the workes (ex: lookup table)
      *
      * Spark sends all variables in closures to the worker nodes, but it can be
      * inefficient: spark is optimized for sending small tasks and you might use
      * the var in several places but spark sends it each time.
      *
      * Note: the variable is sent to worker nodes only once and isn't updated
      *
      * To make sure it's read-only, use inmutable object. Otherwise you have to
      * enforce read-only manually
      */

    /**
      * An example of why it's inefficient: looking up countries from the callsigns
      * from an array. This runs, but if the table's big, sending it for each task
      * is inefficient.
      */

    /**
      * As a last point, if you're broadcasting large values, you have to make sure
      * you use an efficient serialization library. Java's default is bad, spark
      * has one and there's another called Kryo.
      */

    val signPrefixes = sc.broadcast(loadCallSignTable()) // create broadcast variable

    val countryContactCounts = contactCounts.map{ case(sign, count) =>
      val country = lookupInArray(sign, signPrefixes.value) // access the variable with the value property
      (country, count)
    }.reduceByKey((x, y) => x + y)

  }

  def loadCallSignTable() ={
    scala.io.Source.fromFile("D:/data/callsign_tbl_sorted").getLines().filter(_ != "").toArray
  }

  def lookupInArray(sign: String, prefixArray: Array[String]) = {
    val pos = java.util.Arrays.binarySearch(prefixArray.asInstanceOf[Array[AnyRef]], sign) match {
      case x if x < 0 => -x-1
      case x => x
    }
    prefixArray(pos).split(",")(1)
  }
}
