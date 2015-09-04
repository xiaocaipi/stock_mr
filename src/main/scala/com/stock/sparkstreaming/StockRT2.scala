package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import com.stock.util.ScalaCommonUtil
import org.apache.log4j.{Level, Logger}
import com.google.gson.GsonBuilder
import com.stock.dto.StockRealTimeData
import com.stock.util.CommonUtil
import com.stock.hbase.HbaseClientUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StockRT2 {

 val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
 var  connection:HConnection = null;
 var  table:HTableInterface = null;
 
 
 def openHBase(tablename:String) : Unit = {
   val conf = ScalaCommonUtil.getconf;
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
 
    val zkQuorum = ScalaCommonUtil.getPropertyValue("zkQuorum")
    val topics = ScalaCommonUtil.getPropertyValue("topics")
    val group = ScalaCommonUtil.getPropertyValue("group")
    val numThreads = ScalaCommonUtil.getPropertyValue("numThreads")
    val principal = ScalaCommonUtil.getPropertyValue("principal")
	val keytabPath = ScalaCommonUtil.getPropertyValue("keytabPath")
	val krbconfPath = ScalaCommonUtil.getPropertyValue("krbconfPath")
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