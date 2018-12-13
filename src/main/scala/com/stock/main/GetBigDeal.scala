package com.stock.main

import java.io.File
import java.math.BigDecimal

import com.stock.hbase.HbaseService
import com.stock.util.{SortUtil, StockCommonUtil}
import com.stock.vo.{StockData, StockRealTimeData}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import util.DateUtil
import java.util
import java.util.{Arrays, Collections, Comparator, Properties}

import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import scala.collection.JavaConversions._

//    args(1)="hdfs://hadoop3:8020/s/data/origin_rt/"
object GetBigDeal {

  val logger = Logger.getRootLogger

  def main(args: Array[String]) {

    if (args.length != 1) {
      return
    }
    var date = args(0)
//            val conf = new SparkConf().setAppName("GetBigDeal").setMaster("local[10]")
    val conf = new SparkConf().setAppName("GetBigDeal")
    conf.set("spark.default.parallelism", "20")
    conf.set("spark.yarn.executor.memoryOverhead", "2048")
    val zsCode = "399006"

    val sc = new SparkContext(conf)


    var originRtFile = "hdfs://cdh:8020/user/root/stock/rt/" + date + ".tar.gz"
    val holidayRdd = getStockHolidayRdd(sc);
//    StockCommonUtil.myPrint(holidayRdd,5)
    val holidayList = new util.ArrayList[String]();
    holidayRdd.collect().foreach(x=>holidayList.add(x))


    val lastTradeDate = DateUtil.getTradeDayByN(date, 1,holidayList)





    // 获取昨天的k线数据
    val code2TodayStockDataRdd = getStockBaseRdd(sc, lastTradeDate)
    //{"time":"14:31:00","buy1shou":218670.0,"buy2shou":104400.0,"buy3shou":105600.0,"buy4shou":124700.0,"buy5shou":25700.0,"buy1price":16.93,"buy2price":16.92,"buy3price":16.91,"buy4price":16.9,"buy5price":16.89,"sell1shou":128390.0,"sell2shou":345600.0,"sell3shou":228850.0,"sell4shou":97820.0,"sell5shou":173990.0,"sell1price":16.94,"sell2price":16.95,"sell3price":16.96,"sell4price":16.97,"sell5price":16.98,"dealSumShou":4.1214547E7,"dealnowShou":0.0,"refclose":16.68,"batchId":"2016-11-231","code":"600000","name":"浦发银行","open":16.68,"high":17.23,"low":16.66,"close":16.94,"volume":0.0,"adj":0.0,"zhangdie":0.26000000000000156,"zhangdiefudu":1.5587529976019279,"dealmoney":7.01023113E8,"huanshoulv":0.0}
    //获取实时数据
    val originStockRealTimeDataRdd = StockCommonUtil.getOriginStockRealTimeDataRdd(originRtFile, sc)
    val code2originStockRealTimeDataRdd = getCode2originStockRealTimeDataRdd(originStockRealTimeDataRdd)


    //获取 每一笔单子 比上 昨日 总成交额的比例
    val code2NowBatchProportion = code2originStockRealTimeDataRdd.join(code2TodayStockDataRdd).map { joinThing =>
      val code = joinThing._1
      val lastDayVolume = joinThing._2._2.getVolume
      val nowBatchVoulume = joinThing._2._1.getDealnowShou
      val nowBatchproportion = StockCommonUtil.mathOperatorInDouble(nowBatchVoulume, lastDayVolume, "div")
      val batchId = joinThing._2._1.getBatchId
      val currentClose = joinThing._2._1.getClose;
      val time = joinThing._2._1.getTime
      (code, nowBatchproportion + "--" + batchId + "--" + currentClose + "--" + time)
    }


    //获得大单，取一个股票前20比大单  和 这个大单出现后 后面60笔  平均涨幅
    val code2Top20List = code2NowBatchProportion.groupByKey().map { groupThing =>
      val code = groupThing._1
      var nowBatchproportionAndbatchIdMap = scala.collection.mutable.Map[java.lang.Long, BigDecimal]()
      val nowBatchproportionAndbatchIdList = new util.ArrayList[String]()
      for (nowBatchproportionAndbatchId <- groupThing._2) {
        nowBatchproportionAndbatchIdList.add(nowBatchproportionAndbatchId)
        val proportion = new BigDecimal(nowBatchproportionAndbatchId.split("--")(0))
        val batchIdTmp = nowBatchproportionAndbatchId.split("--")(1)
        var batchId = StockCommonUtil.getBatchId(batchIdTmp)
        val currentClose = new BigDecimal(nowBatchproportionAndbatchId.split("--")(2))
        nowBatchproportionAndbatchIdMap += (batchId -> currentClose)
      }
      SortUtil.sortCollection(nowBatchproportionAndbatchIdList)
      Collections.reverse(nowBatchproportionAndbatchIdList)
      var i = 0
      import scala.util.control.Breaks._

      val top20List = new util.ArrayList[String]()
      breakable {
        for (nowBatchproportionAndbatchId <- nowBatchproportionAndbatchIdList) {
          i = i + 1
          if (i > 20) {
            break()
          }
          top20List.add(nowBatchproportionAndbatchId)
        }
      }
      //这个大单出现后 后面60笔  平均涨幅
      var batchIdPorAvgStringList = scala.collection.mutable.ArrayBuffer[String]()
      for (nowBatchproportionAndbatchId <- top20List) {
        var batchIdTmp = nowBatchproportionAndbatchId.split("--")(1)
        var proportion = nowBatchproportionAndbatchId.split("--")(0)
        var time = nowBatchproportionAndbatchId.split("--")(3)
        var batchId = StockCommonUtil.getBatchId(batchIdTmp)
        val currentClose = new BigDecimal(nowBatchproportionAndbatchId.split("--")(2))
        var laterCloseSum = currentClose
        var num = 0
        breakable {
          for (j <- 1 to 60) {
            num = j
            val nextBatchId = batchId + j
            if (nowBatchproportionAndbatchIdMap.contains(nextBatchId)) {
              val tmpClose = nowBatchproportionAndbatchIdMap(nextBatchId)
              laterCloseSum = laterCloseSum.add(tmpClose)
            } else {
              break()
            }

          }
        }

        var laterAvgClose = StockCommonUtil.mathOperatorInBigDecimal(laterCloseSum, new BigDecimal(num), "div")
        var laterAvgZhangfu = StockCommonUtil.mathOperatorInBigDecimal(laterAvgClose, currentClose, "div").subtract(new BigDecimal(1))

        batchIdPorAvgStringList += (batchId + "--" + proportion + "--" + laterAvgZhangfu + "--" + time + "--" + code)
      }

      batchIdPorAvgStringList


    }

    val rowRdd = code2Top20List.flatMap(x => x).map { x =>
      val batchid = x.split("--")(0)
      val proportion = x.split("--")(1)
      val laterAvgZhangfu = x.split("--")(2)
      val time = x.split("--")(3)
      val code = x.split("--")(4)
      Row(code, date, time, proportion, laterAvgZhangfu)
    }


    //    StockCommonUtil.myPrint(code2RtRdd, 5)
//    var action_rows = scala.collection.mutable.ListBuffer[Row]()
//    val row : Row = RowFactory.create("000831","2017-09-01","09:23:03","0.76","0.078")
//    action_rows += row
//    val rowRdd1= sc.parallelize(action_rows)

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")



    val schema: StructType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("code", DataTypes.StringType, true),
      DataTypes.createStructField("trade_date", DataTypes.StringType, true),
      DataTypes.createStructField("time", DataTypes.StringType, true),
      DataTypes.createStructField("portation", DataTypes.StringType, true),
      DataTypes.createStructField("later_avg_zhangfu", DataTypes.StringType, true)))

    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

    val df: DataFrame = sqlcontext.createDataFrame(rowRdd, schema)

    df.write.mode("append").jdbc("jdbc:mysql://db_mysql:3306/kms?useUnicode=true&characterEncoding=UTF-8", "stock_big_deal", properties)


  }


  def getCode2originStockRealTimeDataRdd(originStockRealTimeDataRdd: RDD[StockRealTimeData]): RDD[(String, StockRealTimeData)] = {
    originStockRealTimeDataRdd.map { originStockRealTimeDataRdd =>
      (originStockRealTimeDataRdd.getCode, originStockRealTimeDataRdd)
    }
  }


  def getStockBaseRdd(sc: SparkContext, date: String): RDD[(String, StockData)] = {

    val map1 = Map("url" -> "jdbc:mysql://db_mysql:3306/kms?user=root&password=", "dbtable" -> "base_stock", "driver" -> "com.mysql.jdbc.Driver")
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlcontext.read.format("jdbc").options(map1).load()
    df.registerTempTable("base_stock")
    val sql = "select code ,close,volume from base_stock where date =" + "'" + date + "'"
    val baseStockRdd = sqlcontext.sql(sql)
    val codeTodyStockDataRdd: RDD[(String, StockData)] = baseStockRdd.map { baseStock =>
      val stockData = new StockData()
      val code = baseStock.getString(0)
      val todayClose = baseStock.getDouble(1)
      val todayVolume = baseStock.getDouble(2)
      stockData.setClose(todayClose)
      stockData.setVolume(todayVolume)
      (code, stockData)
    }
    codeTodyStockDataRdd


  }


  def getStockHolidayRdd(sc: SparkContext): RDD[String] = {

    val map1 = Map("url" -> "jdbc:mysql://db_mysql:3306/kms?user=root&password=", "dbtable" -> "stock_holiday", "driver" -> "com.mysql.jdbc.Driver")
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlcontext.read.format("jdbc").options(map1).load()
    df.registerTempTable("stock_holiday")
    val sql = "select date from stock_holiday "
    val baseStockRdd = sqlcontext.sql(sql)
    val codeTodyStockDataRdd: RDD[String] = baseStockRdd.map { baseStock =>
      val stockData = new StockData()
      val date = baseStock.getString(0)
      date
    }
    codeTodyStockDataRdd


  }

}



