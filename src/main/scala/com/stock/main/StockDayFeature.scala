package com.stock.main

import java.io.File

import com.stock.vo.StockData
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.CommonUtil
import java.util
import java.util.Collections

import com.stock.util.{SortUtil, StockCommonUtil}
import org.apache.commons.io.FileUtils

//   date  code  zf1day   zf3day   zf5day  zf10day
object StockDayFeature {

  val logger = Logger.getRootLogger

  def main(args: Array[String]) {

    //    if (args.length != 3) {
    //      return
    //    }
    //    var tableName = args(0)
    //    val isShoudCreateTable = args(1)
    //    val isGolbal = args(2)

    val outputPath = "/tmp/spark/preparert/"
    FileUtils.forceDeleteOnExit(new File(outputPath))
    val conf = new SparkConf().setAppName("mysql2hive")
    //        val conf = new SparkConf().setAppName("mysql2hive")
    conf.set("spark.default.parallelism", "10")
    conf.set("spark.yarn.executor.memoryOverhead", "512")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val thisDate = CommonUtil.getCurrentDate
    val zsCode = "399006"

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._

    val stockDataRdd = getBaseStockRddAll(hiveContext)
    val code2stockDataRdd  = stockDataRdd.map(x => (x.getCode, x))
    val zsStockDataList = new util.ArrayList[StockData]()
    code2stockDataRdd.filter(x=>x._1.equals(zsCode)).collect().foreach(x=>zsStockDataList.add(x._2))
    SortUtil.sortStockDataCollection(zsStockDataList)
    Collections.reverse(zsStockDataList)

    val zsStockFeatureArrayBuffer =  getStockFeature(zsStockDataList)
    val zsDate2StockFeatureMap = scala.collection.mutable.Map[String,StockFeature]()
    zsStockFeatureArrayBuffer.foreach(x=>zsDate2StockFeatureMap +=(x.date -> x))
    val zsDate2StockFeatureMapValue = sc.broadcast(zsDate2StockFeatureMap)

//    println (zsStockFeatureArrayBuffer)



    val stockFeatureAllArrayBufferRdd = code2stockDataRdd.groupByKey().map { x =>
      val code = x._1
      val stockDataList = new util.ArrayList[StockData]()

      for (stockData <- x._2) {
        stockDataList.add(stockData)
      }
      SortUtil.sortStockDataCollection(stockDataList)
      Collections.reverse(stockDataList)
      val stockFeatureArraybuffer = getStockFeature(stockDataList)
      val zsStockFeatureBcMap= zsDate2StockFeatureMapValue.value
      val stockFeatureAllArrayBuffer = scala.collection.mutable.ArrayBuffer[StockFeatureAll]()
      stockFeatureArraybuffer.foreach{stockFeature=>
        val date = stockFeature.date
        if(zsStockFeatureBcMap.contains(date)){
          val zsStockFeature  = zsStockFeatureBcMap(date)
          val xdzf1day =StockCommonUtil.mathOperatorInBigDecimal(stockFeature.zf1day,zsStockFeature.zf1day,"div")
          val xdzf3day =StockCommonUtil.mathOperatorInBigDecimal(stockFeature.zf3day,zsStockFeature.zf1day,"div")
          val xdzf5day =StockCommonUtil.mathOperatorInBigDecimal(stockFeature.zf5day,zsStockFeature.zf1day,"div")
          val xdzf10day =StockCommonUtil.mathOperatorInBigDecimal(stockFeature.zf10day,zsStockFeature.zf1day,"div")
          val xdzf20day =StockCommonUtil.mathOperatorInBigDecimal(stockFeature.zf20day,zsStockFeature.zf1day,"div")
          val stockFeatureAll = StockFeatureAll(code,date,stockFeature.zf1day,stockFeature.zf3day,stockFeature.zf5day,stockFeature.zf10day,stockFeature.zf20day,
            stockFeature.vzf1day,stockFeature.vzf3day,stockFeature.vzf5day,stockFeature.vzf10day,stockFeature.vzf20day,xdzf1day,xdzf3day,xdzf5day,xdzf10day,xdzf20day,
            stockFeature.open,stockFeature.close,stockFeature.volume,stockFeature.dealMoney,stockFeature.label)
          stockFeatureAllArrayBuffer += stockFeatureAll
        }

      }
      stockFeatureAllArrayBuffer

    }

    val stockFeatureAllRdd =stockFeatureAllArrayBufferRdd.flatMap(x=>x)
    stockFeatureAllRdd.repartition(1).saveAsTextFile(outputPath)
//    StockCommonUtil.myPrint(stockFeatureAllRdd,5)






  }

  def getStockFeature(stockDataList:util.ArrayList[StockData]):scala.collection.mutable.ArrayBuffer[StockFeature]={

    val stockFeatureArrayBuffer = scala.collection.mutable.ArrayBuffer[StockFeature]()
    import scala.util.control.Breaks._
    breakable {
      for (i <- 1 to stockDataList.size() - 1) {
        val tommorrowIndex = i-1
        val currentIndex = i
        val index1day = i + 1
        val index3day = i + 3
        val index5day =i+5
        val index10day =i+10
        val index20day =i+20


        if (index20day > stockDataList.size() - 1) {
          break()
        }
        val stockData = stockDataList.get(i)
        val code = stockData.getCode
        val date = stockData.getDate
        val volume = StockCommonUtil.getBigDecimalSacle4(stockData.getVolume)
        val close = StockCommonUtil.getBigDecimalSacle4(stockData.getClose)
        val open = StockCommonUtil.getBigDecimalSacle4(stockData.getOpen)
        val dealMoney = StockCommonUtil.getBigDecimalSacle4(stockData.getDealmoney)
        val stockData1day = stockDataList.get(index1day)
        val stockData3day = stockDataList.get(index3day)
        val stockData5day = stockDataList.get(index5day)
        val stockData10day = stockDataList.get(index10day)
        val stockData20day = stockDataList.get(index20day)
        val stockDataTommorrowday = stockDataList.get(tommorrowIndex)
        val zf1day = StockCommonUtil.mathOperatorInDouble(stockData.getClose,stockData1day.getClose,"div")
        val zf3day = StockCommonUtil.mathOperatorInDouble(stockData.getClose,stockData3day.getClose,"div")
        val zf5day = StockCommonUtil.mathOperatorInDouble(stockData.getClose,stockData5day.getClose,"div")
        val zf10day = StockCommonUtil.mathOperatorInDouble(stockData.getClose,stockData10day.getClose,"div")
        val zf20day = StockCommonUtil.mathOperatorInDouble(stockData.getClose,stockData20day.getClose,"div")
        val label = StockCommonUtil.mathOperatorInDouble(stockDataTommorrowday.getClose,stockData.getClose,"div")

        val vzf1day = StockCommonUtil.mathOperatorInDouble(stockData.getVolume,stockData1day.getVolume,"div")
        val vzf3day = StockCommonUtil.mathOperatorInDouble(stockData.getVolume,stockData3day.getVolume,"div")
        val vzf5day = StockCommonUtil.mathOperatorInDouble(stockData.getVolume,stockData5day.getVolume,"div")
        val vzf10day = StockCommonUtil.mathOperatorInDouble(stockData.getVolume,stockData10day.getVolume,"div")
        val vzf20day = StockCommonUtil.mathOperatorInDouble(stockData.getVolume,stockData20day.getVolume,"div")

        val stockFeature = StockFeature(code,date,zf1day,zf3day,zf5day,zf10day,zf20day,vzf1day,vzf3day,vzf5day,vzf10day,vzf20day,open,close,volume,dealMoney,label)
        stockFeatureArrayBuffer += stockFeature
      }
    }
    stockFeatureArrayBuffer

  }


  def getBaseStockRddAll(hiveContext: HiveContext): RDD[StockData] = {

    hiveContext.sql("use ods")
    val querySql = "select code ,close,date,volume,open,dealmoney from base_stock"
    val rdd = hiveContext.sql(querySql).map { x =>
      val stockData = new StockData()
      stockData.setCode(x.getString(0))
      stockData.setClose(new java.math.BigDecimal(x.getString(1)).doubleValue())
      stockData.setDate(x.getString(2))
      stockData.setVolume(new java.math.BigDecimal(x.getString(3)).doubleValue())
      stockData.setOpen(new java.math.BigDecimal(x.getString(4)).doubleValue())
      stockData.setDealmoney(new java.math.BigDecimal(x.getString(5)).doubleValue())
      stockData
    }
    rdd

  }


  case class StockFeature(code: String,
                          date: String,
                          zf1day: java.math.BigDecimal,
                          zf3day:java.math.BigDecimal,
                          zf5day:java.math.BigDecimal,
                          zf10day:java.math.BigDecimal,
                          zf20day:java.math.BigDecimal,
                          vzf1day:java.math.BigDecimal,
                          vzf3day:java.math.BigDecimal,
                          vzf5day:java.math.BigDecimal,
                          vzf10day:java.math.BigDecimal,
                          vzf20day:java.math.BigDecimal,
                          open:java.math.BigDecimal,
                          close:java.math.BigDecimal,
                          volume:java.math.BigDecimal,
                          dealMoney:java.math.BigDecimal,
                          label:java.math.BigDecimal)


  case class StockFeatureAll(code: String,
                             date: String,
                             zf1day: java.math.BigDecimal,
                             zf3day:java.math.BigDecimal,
                             zf5day:java.math.BigDecimal,
                             zf10day:java.math.BigDecimal,
                             zf20day:java.math.BigDecimal,
                             vzf1day:java.math.BigDecimal,
                             vzf3day:java.math.BigDecimal,
                             vzf5day:java.math.BigDecimal,
                             vzf10day:java.math.BigDecimal,
                             vzf20day:java.math.BigDecimal,
                             xdzf1day:java.math.BigDecimal,
                             xdzf3day:java.math.BigDecimal,
                             xdzf5day:java.math.BigDecimal,
                             xdzf10day:java.math.BigDecimal,
                             xdzf20day:java.math.BigDecimal,
                             open:java.math.BigDecimal,
                             close:java.math.BigDecimal,
                             volume:java.math.BigDecimal,
                             dealMoney:java.math.BigDecimal,
                             label:java.math.BigDecimal)



}



