package io

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner}

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

object Io {
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

    // reading a simple text file:
    val textFile = sc.textFile("D:/data/shakespeare.txt")

    // same method is good for reading partitioned files:
    // 2 files here: shakespear_1.txt shakespear_2.txt, they will get UNIONED
    val mergedTextFiles = sc.textFile("D:/data/shakespeare/")

    // if files small you get back a pairRDD with names:
    val mergedFilesPairRDD = sc.wholeTextFiles("D:/data/shakespeare/")

    println(mergedFilesPairRDD.collect().mkString(" "))

    // reading csv file
    var csv = sc.textFile("D:/data/csv_data.csv")
    val csvResult = csv.map{ line =>
      val reader  = new CSVReader(new StringReader(line));
      reader.readNext();
      //line
    }

    println(csvResult.collect().foreach(println))
  }
}
