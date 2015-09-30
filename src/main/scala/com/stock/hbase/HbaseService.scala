package com.stock.hbase

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import com.google.gson.GsonBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import java.util.ArrayList
import com.stock.vo.StockRealTimeData
import util.CommonUtil
import com.stock.util.HbaseClientUtil
import scala.collection.mutable.ListBuffer
import com.stock.vo.StockAlertVo

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
    HbaseClientUtil.insertByObject(stockrealTimeData, "test_rt", "rtf")
  }
  
  def convertMessage(message:String):StockRealTimeData ={
    val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    val stockrealTimeData  = gson.fromJson(message,classOf[StockRealTimeData]);
    stockrealTimeData 
  }
  
  def insertStockRTDataList(stockList:ListBuffer[StockRealTimeData]):String ={
      val conf =CommonUtil.getConf("1")
      val table = new HTable(conf, "test_rt2");
      var putList = new ArrayList[Put]()
      for( stockrealtimedata <- stockList){
         if(stockrealtimedata.getDealnowShou()>0){
	         val code = stockrealtimedata.getCode()
	         val time = stockrealtimedata.getTime()
	         val currentDate = CommonUtil.getCurrentDate;
		     val rowkey = code+"_"+currentDate+" "+time
		     stockrealtimedata.setRowkey(rowkey)
	         val put = HbaseClientUtil.obj2put(stockrealtimedata, "test_rt2", "rtf")
	         putList.add(put) 
         }
      }
      table.put(putList)
      table.flushCommits();
     
    "1" 
  }
  
  def getStockAlert():scala.collection.mutable.Map[String, StockAlertVo] ={
    val list = HbaseClientUtil.getStockAlertList();
    val returnMap =  scala.collection.mutable.Map[String,StockAlertVo]()
    for(i <- 0 to list.size()-1){
      returnMap += (list.get(i).getCode() -> list.get(i))
    }
    returnMap
   
  }
  
  
  

}