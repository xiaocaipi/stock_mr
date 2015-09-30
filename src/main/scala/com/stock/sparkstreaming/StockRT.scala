package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import util.CommonUtil

object StockRT {


  def main(args: Array[String]) {

	  Logger.getRootLogger.setLevel(Level.ERROR)
 
    val zkQuorum = CommonUtil.getPropertyValue("zkQuorum")
    val topics = CommonUtil.getPropertyValue("topics")
    val group = CommonUtil.getPropertyValue("group")
    val numThreads = CommonUtil.getPropertyValue("numThreads")
    val principal = CommonUtil.getPropertyValue("principal")
	val keytabPath = CommonUtil.getPropertyValue("keytabPath")
	val krbconfPath = CommonUtil.getPropertyValue("krbconfPath")
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