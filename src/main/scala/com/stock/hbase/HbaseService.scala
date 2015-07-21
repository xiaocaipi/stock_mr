package com.stock.hbase

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.stock.dto.StockRealTimeData

object HbaseService {
  
  def insertStockRTData(message:String) : Unit = {
    val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    val stockrealTimeDatatmp =  new StockRealTimeData()
    val stockrealTimeData  = gson.fromJson(message,stockrealTimeDatatmp.getClass());
    val code = stockrealTimeData.getCode()
    val time =  stockrealTimeData.getTime()
    val rowkey = code+"_"+time
    stockrealTimeData.setRowkey(rowkey)
    HbaseClientUtil.insertByObject(stockrealTimeData, "test_rt", "rtf")
  }
  
  

}