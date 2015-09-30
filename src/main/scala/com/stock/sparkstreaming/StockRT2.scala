package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{Level, Logger}
import com.google.gson.GsonBuilder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import util.CommonUtil
import com.stock.vo.StockRealTimeData

object StockRT2 {

 val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
 var  connection:HConnection = null;
 var  table:HTableInterface = null;
 
 
 def openHBase(tablename:String) : Unit = {
   val conf = CommonUtil.getConf("1");
   val hConnection = classOf[HConnection]
	   hConnection.synchronized{
	     if (connection == null){
	         connection= HConnectionManager.createConnection(conf)
	     }
	   }
   val hTableInterfaceClass = classOf[HTableInterface]
	   hTableInterfaceClass.synchronized{
	     if (table == null){
	       table = connection.getTable("test_rt");
	     }
	   }
   
  
}
  def main(args: Array[String]) {

    
//	  StreamingExamples.setStreamingLogLevels
//	  Logger.getRootLogger.setLevel(Level.WARN)
 
    val zkQuorum = CommonUtil.getPropertyValue("zkQuorum")
    val topics = CommonUtil.getPropertyValue("topics")
    val group = CommonUtil.getPropertyValue("group")
    val numThreads = CommonUtil.getPropertyValue("numThreads")
    val principal = CommonUtil.getPropertyValue("principal")
	val keytabPath = CommonUtil.getPropertyValue("keytabPath")
	val krbconfPath = CommonUtil.getPropertyValue("krbconfPath")
	System.setProperty("java.security.krb5.conf", krbconfPath)
  
    openHBase("");
//    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[8]")
	  val sparkConf = new SparkConf().setAppName("stockRt")
      sparkConf.set("spark.yarn.keytab", keytabPath)
    sparkConf.set("spark.yarn.principal", principal)
    
    
    val ssc = new StreamingContext(sparkConf, Seconds(1))
	  
 	
 
// 	initialHbase()

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD(rdd => {
    rdd.foreach(record => {
       
	    val stockrealTimeDatatmp =  new StockRealTimeData()
	    val stockrealTimeData  = gson.fromJson(record,stockrealTimeDatatmp.getClass());
	    val code = stockrealTimeData.getCode()
	    val time =  stockrealTimeData.getTime()
	    val time1 =CommonUtil.getCurrentTime()
	    val rowkey = code+"_"+time1
	    stockrealTimeData.setRowkey(rowkey)
    	val put = new Put(Bytes.toBytes(rowkey));
    	put.add(Bytes.toBytes("rtf"), Bytes.toBytes("test"), Bytes.toBytes("1"));
    	table.put(put);
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
  

}