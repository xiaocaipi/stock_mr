package com.stock.util

import java.math.BigDecimal

import com.stock.hbase.HbaseService
import com.stock.vo.StockRealTimeData
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object StockCommonUtil {


  def getOriginStockRealTimeDataRdd(hdfsPath: String, sc: SparkContext): RDD[StockRealTimeData] = {
    val origintext = sc.textFile(hdfsPath).repartition(10).filter(_.contains("{"))
    val originStockRealTimeDataRdd = origintext.map { x =>
      val stockRealTimeData: StockRealTimeData = HbaseService.convertMessage(x)
      stockRealTimeData
    }.filter {
      _.getClose > 0
    }
    originStockRealTimeDataRdd
  }


  def myPrint(rdd: RDD[_], num: Int): Unit = {
    rdd.take(num).foreach(println)
  }

  def getBigDecimalSacle4(obj: Any): BigDecimal = {
    val returnValue = new BigDecimal(String.valueOf(obj)).setScale(4, BigDecimal.ROUND_HALF_UP)
    returnValue
  }

  def getBigDecimalSacle4ByDecimal(obj: BigDecimal): BigDecimal = {
    val returnValue = obj.setScale(4, BigDecimal.ROUND_HALF_UP)
    returnValue
  }

  //参数是double 类型的  运算
  def mathOperatorInDouble(obj1: java.lang.Double, obj2: java.lang.Double, operatior: String): BigDecimal = {
    var returnvalue = new BigDecimal(0)
    if (operatior.equals("div")) {
      val operatorValue = new BigDecimal(obj1).divide(new BigDecimal(obj2), 4, BigDecimal.ROUND_HALF_UP)
      returnvalue = getBigDecimalSacle4ByDecimal(operatorValue)
    } else if (operatior.equals("add")) {
      val operatorValue = new BigDecimal(obj1).add(new BigDecimal(obj2))
      returnvalue = getBigDecimalSacle4ByDecimal(operatorValue)
    } else if (operatior.equals("sub")) {
      val operatorValue = new BigDecimal(obj1).subtract(new BigDecimal(obj2))
      returnvalue = getBigDecimalSacle4ByDecimal(operatorValue)
    } else if (operatior.equals("mul")) {
      val operatorValue = new BigDecimal(obj1).multiply(new BigDecimal(obj2))
      returnvalue = getBigDecimalSacle4ByDecimal(operatorValue)
    }

    returnvalue

  }

  def mathOperatorInBigDecimal(obj1: BigDecimal, obj2: BigDecimal, operator: String): BigDecimal = {
    val returnvalue = mathOperatorInDouble(obj1.doubleValue(), obj2.doubleValue(), operator)
    returnvalue
  }

  def getBatchId(batchId: String): java.lang.Long = {
    //2017-10-09117652
    val date = batchId.substring(0, 10)
    var returnValue = batchId.replace(date, "")
    java.lang.Long.parseLong(returnValue)

  }


}
