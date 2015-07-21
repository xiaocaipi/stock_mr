package com.stock.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.Seconds
import com.lufax.util.ScalaCommonUtil
import com.stock.hbase.HbaseService
import com.lufax.streaming.StreamingExamples
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

object StockRT {


  def main(args: Array[String]) {


    	
    val zkQuorum = ScalaCommonUtil.getPropertyValue("zkQuorum")
    val topics = ScalaCommonUtil.getPropertyValue("topics")
    val group = ScalaCommonUtil.getPropertyValue("group")
    val numThreads = ScalaCommonUtil.getPropertyValue("numThreads")
    StreamingExamples.setStreamingLogLevels()
    initialHbase()

    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[8]")
//    val sparkConf = new SparkConf().setAppName("stockRt")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD(rdd => {
      rdd.foreach(record => {
        HbaseService.insertStockRTData(record)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
  
  def initialHbase() : Unit = {
    val conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "hdfs://hadoop3:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "hadoop3,hadoop5,hadoop2,hadoop1,hadoop4")

    val admin = new HBaseAdmin(conf);
    val tableName = "test_rt"
    val family = "rtf"
    if (!admin.tableExists(tableName)) {
            val tabledes = new HTableDescriptor(tableName);
                val hd = new HColumnDescriptor(family);
                hd.setMaxVersions(1);
                tabledes.addFamily(hd);
            admin.createTable(tabledes);
        }
  
}

}