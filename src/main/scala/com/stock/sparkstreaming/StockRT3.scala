package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import com.stock.util.ScalaCommonUtil
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer
import com.stock.dto.StockRealTimeData
import com.stock.util.CommonUtil

object StockRT3 {


  def main(args: Array[String]) {

	  Logger.getRootLogger.setLevel(Level.ERROR)
 
    val zkQuorum = ScalaCommonUtil.getPropertyValue("zkQuorum")
    val topics = ScalaCommonUtil.getPropertyValue("topics")
    val group = ScalaCommonUtil.getPropertyValue("group")
    val numThreads = ScalaCommonUtil.getPropertyValue("numThreads")
    val principal = ScalaCommonUtil.getPropertyValue("principal")
	val keytabPath = ScalaCommonUtil.getPropertyValue("keytabPath")
	val krbconfPath = ScalaCommonUtil.getPropertyValue("krbconfPath")
//    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[8]")
    val sparkConf = new SparkConf().setAppName("stockRt")
    
//    sparkConf.set("java.security.krb5.realm", krbconfPath)
    System.setProperty("java.security.krb5.conf", krbconfPath)
    sparkConf.set("spark.yarn.keytab", keytabPath)
    sparkConf.set("spark.yarn.principal", principal)
    sparkConf.set("spark.default.parallelism", "16")
    
    
    val ssc = new StreamingContext(sparkConf, Seconds(3))
	  
 	
 
// 	initialHbase()
	
    val topicMap = topics.split(",").map((_, 2.toInt)).toMap
    val numStreams = 4
    val kafkaStreams = (1 to numStreams).map { i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) }
	val lines = ssc.union(kafkaStreams)
//    val words = lines.flatMap(_.split(" "))
    
    //dstream(lines) -->  rdds(rdd) -->partitionrdds(partitionOfRecords) -->rdd  (record)   			
    lines.foreachRDD(rdds => {
     
      rdds.foreachPartition(partitionOfRecords =>{
         var stockRealTimeDataList = new ListBuffer[StockRealTimeData]()
         partitionOfRecords.foreach(record => {
            var stockRealTimeData = HbaseService.convertMessage(record)
	        stockRealTimeDataList += stockRealTimeData
         })
         HbaseService.insertStockRTDataList(stockRealTimeDataList)
      })
     
    })
    ssc.start()
    ssc.awaitTermination()
  }
  

}