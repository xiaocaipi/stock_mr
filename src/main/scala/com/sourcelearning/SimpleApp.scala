package com.sourcelearning

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object SimpleApp {
  def main(args: Array[String]) {
	val input = args(0)
	val saveFile = args(1);
    val logFile = input
    val conf = new SparkConf().setAppName("Scala Application")
    val sc = new SparkContext(conf)
    val file = sc.textFile(logFile)
    val count = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    count.collect()
    count.saveAsTextFile(saveFile)
  }
}
