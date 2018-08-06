package chapter6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Example2 {
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

    spark.conf.set("spark.sql.crossJoin.enabled", "true") // to enable cartesian products

    val file = sc.textFile("D:/data/callsigns.txt")

    /**
      * we can use accumulators for validation as well
      */

    // let's look at an accumulator for counting blank lines in the callsigns file

    // create an accumulator initialized to 0
    val validCount = sc.longAccumulator("valid")
    val invalidCount = sc.longAccumulator("invalid")

    file.foreach { line =>
      if(line.matches(raw"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z")) {
        validCount.add(1)
      }else{
        invalidCount.add(1)
      }
    }

    /**
      * worker nodes can't access accumulator's values, this is doen for efficiency
      * good for when you need to trace several values
      */
    //callSigns.saveAsTextFile("output.txt")
    println(s"Valids: $validCount")
    println(s"Invalids: $invalidCount")

  }
}
