package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{ Level, Logger }
import scala.collection.mutable.ListBuffer
import util.CommonUtil
import com.stock.vo.StockRealTimeData
import com.stock.service.StockRTService

object StockRT3 {

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.ERROR)

    val zkQuorum = CommonUtil.getPropertyValue("zkQuorum")
    val topics = CommonUtil.getPropertyValue("topics")
    val group = CommonUtil.getPropertyValue("group")
    val numThreads = CommonUtil.getPropertyValue("numThreads")
    val principal = CommonUtil.getPropertyValue("principal")
    val keytabPath = CommonUtil.getPropertyValue("keytabPath")
    val krbconfPath = CommonUtil.getPropertyValue("krbconfPath")
//    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[8]")
        val sparkConf = new SparkConf().setAppName("stockRt")

    //    sparkConf.set("java.security.krb5.realm", krbconfPath)
//    System.setProperty("java.security.krb5.conf", krbconfPath)
//    sparkConf.set("spark.yarn.keytab", keytabPath)
//    sparkConf.set("spark.yarn.principal", principal)
    sparkConf.set("spark.default.parallelism", "16")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    var alertMap = HbaseService.getStockAlert;
    var alertBroad = ssc.sparkContext.broadcast(alertMap)

    // 	initialHbase()

    val topicMap = topics.split(",").map((_, 2.toInt)).toMap
    val numStreams = 4
    val kafkaStreams = (1 to numStreams).map { i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) }
    val lines = ssc.union(kafkaStreams)
    //    val words = lines.flatMap(_.split(" "))

    //dstream(lines) -->  rdds(rdd) -->partitionrdds(partitionOfRecords) -->rdd  (record)   			
    lines.foreachRDD(rdds => {

      rdds.foreachPartition(partitionOfRecords => {
        var stockRealTimeDataList = new ListBuffer[StockRealTimeData]()
        partitionOfRecords.foreach(record => {
          var stockRealTimeData = HbaseService.convertMessage(record)
          val isAlert = StockRTService.isNeedAlert(stockRealTimeData, alertBroad.value)
          if (isAlert) {
            println(stockRealTimeData.getCode() + "--" + stockRealTimeData.getClose())
            StockRTService.alert(stockRealTimeData)
          }
          stockRealTimeDataList += stockRealTimeData
        })
        HbaseService.insertStockRTDataList(stockRealTimeDataList)
      })
      alertMap = HbaseService.getStockAlert;
      alertBroad = ssc.sparkContext.broadcast(alertMap)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}