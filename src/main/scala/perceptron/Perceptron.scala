package perceptron

import org.apache.log4j.Logger
import org.apache.log4j.Level



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Perceptron {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sc = new SparkContext(conf)

    val ss = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    import ss.implicits._

    //val train = sc.textFile("D:/data/perceptron.txt").toDF()

    val df = ss.read.format("libsvm")
      .load("D:/data/perceptron.txt")

    println(df.collect().mkString("||"))


    val layers = Array[Int](2, 2, 1, 2)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(4)
      .setSeed(12L)
      .setMaxIter(3)

    val model = trainer.fit(df)

    val result = model.transform(df)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

  }
}
