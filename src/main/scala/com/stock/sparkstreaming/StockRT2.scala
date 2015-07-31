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
import com.stock.hbase.HbaseService
import com.lufax.streaming.StreamingExamples
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import com.stock.util.ScalaCommonUtil

object StockRT2 {


  def main(args: Array[String]) {

 StreamingExamples.setStreamingLogLevels()
    val zkQuorum = ScalaCommonUtil.getPropertyValue("zkQuorum")
    val topics = ScalaCommonUtil.getPropertyValue("topics")
    val group = ScalaCommonUtil.getPropertyValue("group")
    val numThreads = ScalaCommonUtil.getPropertyValue("numThreads")
    val principal = ScalaCommonUtil.getPropertyValue("principal")
	val keytabPath = ScalaCommonUtil.getPropertyValue("keytabPath")
    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[5]")
//    val sparkConf = new SparkConf().setAppName("stockRt")
    sparkConf.set("spark.yarn.keytab", keytabPath)
    sparkConf.set("spark.yarn.principal", principal)
   
    initialHbase()
    val ssc = new StreamingContext(sparkConf, Seconds(1))
 
 	

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD(rdd => {
      rdd.foreach(record => {
        println(record)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
  
  def initialHbase() : Unit = {
    val conf = ScalaCommonUtil.getconf;
//    val conf = HBaseConfiguration.create()

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