package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer
import util.CommonUtil
import com.stock.vo.StockRealTimeData

object StockRT4 {


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
    System.setProperty("java.security.krb5.conf", krbconfPath)
    sparkConf.set("spark.yarn.keytab", keytabPath)
    sparkConf.set("spark.yarn.principal", principal)
    sparkConf.set("spark.default.parallelism", "16")
    
    
    val ssc = new StreamingContext(sparkConf, Seconds(3))
	 
 	
 
	
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