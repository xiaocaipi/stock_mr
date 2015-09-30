package com.stock.service

import com.stock.vo.StockAlertVo
import com.stock.vo.StockRealTimeData
import java.math.BigDecimal
import com.stock.hbase.HbaseService
import util.CommonUtil
import com.stock.util.HbaseClientUtil

object StockRTService {

  def isNeedAlert(stockRealTimeData: StockRealTimeData, alertBorad: scala.collection.mutable.Map[String, StockAlertVo]): Boolean = {
    if (alertBorad.get(stockRealTimeData.getCode()) != None) {
      val alertVo = alertBorad.get(stockRealTimeData.getCode()).getOrElse(null)
      val close = stockRealTimeData.getClose()
      val huanshoulv = stockRealTimeData.getHuanshoulv()
      val zhangdiefudu = stockRealTimeData.getZhangdiefudu()
      if (new BigDecimal(close).compareTo(new BigDecimal(alertVo.getOverprice())) >= 0) {
        return true
      } else if (new BigDecimal(close).compareTo(new BigDecimal(alertVo.getLowprice())) <= -1) {
        return true
      } else if (new BigDecimal(zhangdiefudu).compareTo(new BigDecimal(alertVo.getZhangfu())) >= 0) {
        return true
      } else if (new BigDecimal(zhangdiefudu).compareTo(new BigDecimal(alertVo.getDiefu())) <= -1) {
        return true
      } else if (new BigDecimal(huanshoulv).compareTo(new BigDecimal(alertVo.getHuanshoulv())) >= 0) {
        return true
      }
      false;
    } else {
      false
    }

  }

  def alert(stockRealTimeData: StockRealTimeData) = {
    val stockRealTimeDataTmp = new StockRealTimeData();
    val code = stockRealTimeData.getCode()
    val time = stockRealTimeData.getTime()
    val currentDate = CommonUtil.getCurrentDate;
    val rowkey = code + "_" + currentDate + " " + time
    stockRealTimeDataTmp.setRowkey(rowkey)
    stockRealTimeDataTmp.setCode(code);
    HbaseClientUtil.insertByObject(stockRealTimeDataTmp, "test_alert_result", "base_cf")
    
    val alertVo = new StockAlertVo()
    alertVo.setRowkey(code)
    alertVo.setStatus("1")
    HbaseClientUtil.insertByObject(alertVo, "test_alert", "base_cf")
  }

}