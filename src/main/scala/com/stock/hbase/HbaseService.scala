package com.stock.hbase

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import com.google.gson.GsonBuilder
import com.stock.dto.StockRealTimeData
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import com.stock.util.ScalaCommonUtil
import com.stock.util.CommonUtil
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import java.util.ArrayList

object HbaseService {
  
  def insertStockRTData(message:String) : Unit = {
    val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    val stockrealTimeDatatmp =  new StockRealTimeData()
    val stockrealTimeData  = gson.fromJson(message,stockrealTimeDatatmp.getClass());
    val code = stockrealTimeData.getCode()
    val time =  stockrealTimeData.getTime()
    val time1 = CommonUtil.getCurrentTime()
    val rowkey = code+"_"+time1
    stockrealTimeData.setRowkey(rowkey)
    if("000723".equals(code)){
      println(rowkey)
    }
    HbaseClientUtil.insertByObject(stockrealTimeData, "test_rt", "rtf")
  }
  
  def convertMessage(message:String):StockRealTimeData ={
    val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    val stockrealTimeData  = gson.fromJson(message,classOf[StockRealTimeData]);
    stockrealTimeData 
  }
  
  def insertStockRTDataList(stockList:ListBuffer[StockRealTimeData]):String ={
      val conf =ScalaCommonUtil.getconf
      val table = new HTable(conf, "test_rt");
      var putList = new ArrayList[Put]()
      for( stockrealtimedata <- stockList){
         if(stockrealtimedata.getDealnowShou()>0){
	         val code = stockrealtimedata.getCode()
	         val time = stockrealtimedata.getTime()
		     val rowkey = code+"_"+time
		     stockrealtimedata.setRowkey(rowkey)
	         val put = HbaseClientUtil.obj2put(stockrealtimedata, "test_rt", "rtf")
	         putList.add(put) 
         }
      }
      table.put(putList)
      table.flushCommits();
     
    "1" 
  }
  
  

}