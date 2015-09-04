package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import com.stock.util.ScalaCommonUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

object StockRT {


  def main(args: Array[String]) {

	  Logger.getRootLogger.setLevel(Level.ERROR)
 
    val zkQuorum = ScalaCommonUtil.getPropertyValue("zkQuorum")
    val topics = ScalaCommonUtil.getPropertyValue("topics")
    val group = ScalaCommonUtil.getPropertyValue("group")
    val numThreads = ScalaCommonUtil.getPropertyValue("numThreads")
    val principal = ScalaCommonUtil.getPropertyValue("principal")
	val keytabPath = ScalaCommonUtil.getPropertyValue("keytabPath")
	val krbconfPath = ScalaCommonUtil.getPropertyValue("krbconfPath")
    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[8]")
//    val sparkConf = new SparkConf().setAppName("stockRt")
    
//    sparkConf.set("java.security.krb5.realm", krbconfPath)
    System.setProperty("java.security.krb5.conf", krbconfPath)
    sparkConf.set("spark.yarn.keytab", keytabPath)
    sparkConf.set("spark.yarn.principal", principal)
    sparkConf.set("spark.default.parallelism", "16")
    
    
    val ssc = new StreamingContext(sparkConf, Seconds(2))
	  
 	
 
// 	initialHbase()

    val topicMap = topics.split(",").map((_, 2.toInt)).toMap
    val numStreams = 4
    val kafkaStreams = (1 to numStreams).map { i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap,StorageLevel.MEMORY_AND_DISK).map(_._2) }
	val lines = ssc.union(kafkaStreams)
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    

    words.foreachRDD(rdd => {
      rdd.foreach(record => {
        HbaseService.insertStockRTData(record)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
  

}