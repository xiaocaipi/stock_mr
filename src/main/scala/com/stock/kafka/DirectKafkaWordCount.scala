package com.stock.kafka

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}  
import util.CommonUtil
  
/** 
 * Created by knowpigxia on 15-8-4. 
 */  
object DirectKafkaWordCount {   
  
  
  def processRdd(rdd: RDD[(String, String)]): Unit = {  
    rdd.foreach(println)  
  }  
  
  def main(args: Array[String]) {  
  
    Logger.getLogger("org").setLevel(Level.WARN)  
  
    
     val topics = CommonUtil.getPropertyValue("topics")
    val group = CommonUtil.getPropertyValue("group")
    
    val kafkaBrokers = CommonUtil.getPropertyValue("kafkaBrokers")
  
    // Create context with 2 second batch interval  
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")  
    sparkConf.setMaster("local[*]")  
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")  
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
  
    val ssc = new StreamingContext(sparkConf, Seconds(2))  
  
    // Create direct kafka stream with brokers and topics  
    val topicsSet = topics.split(",").toSet  
    val kafkaParams = Map[String, String](  
      "metadata.broker.list" -> kafkaBrokers,  
      "group.id" -> group,  
      "auto.offset.reset" -> "smallest"  
    )  
  
    val km = new KafkaManager(kafkaParams)  
  
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](  
      ssc, kafkaParams, topicsSet)  
  
    messages.foreachRDD(rdd => {  
      if (!rdd.isEmpty()) {  
        // 先处理消息  
        processRdd(rdd)  
        // 再更新offsets  
        km.updateZKOffsets(rdd)  
      }  
    })  
  
    ssc.start()  
    ssc.awaitTermination()  
  }  
}  