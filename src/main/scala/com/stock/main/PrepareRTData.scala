package com.stock.main

import java.io.File

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import util.CommonUtil
import org.apache.spark.SparkContext
import com.stock.vo.StockRealTimeData
import java.math.BigDecimal

import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import java.util.Arrays

import com.stock.sparkstreaming.SQLContextSingleton
import org.apache.spark.rdd.RDD
import java.lang.Double

import com.stock.util.SparkConfUtil
import com.stock.util.SparkConfUtil.sparkContext
import org.apache.commons.io.FileUtils

//    args(1)="hdfs://hadoop3:8020/s/data/origin_rt/"
object PrepareRTData {

  val logger = Logger.getRootLogger

  def main(args: Array[String]) {

    //	logger.setLevel(Level.ERROR)
    //    if (args.length < 2) {
    //      logger.error("param is not correct")
    //      System.exit(1)
    //    }

    //    val date = args(0)
    //    val originRtPath = args(1);
    //    var originRtFile = originRtPath + date
    var originRtFile = "hdfs://cdh:8020/user/root/stock/rt/2017-09-01_tmp1.txt"
    val date = "2017-09-01"
    val batchNum = 100;

    val conf = new SparkConf().setAppName("PrepareRTData").setMaster("local[4]")
    conf.set("spark.default.parallelism", "10")
    val zsCode = "399006"
    val sc = new SparkContext(conf)

    val code2TodyCloseRdd = getStockBaseRdd(sc, date)
    //{"time":"14:31:00","buy1shou":218670.0,"buy2shou":104400.0,"buy3shou":105600.0,"buy4shou":124700.0,"buy5shou":25700.0,"buy1price":16.93,"buy2price":16.92,"buy3price":16.91,"buy4price":16.9,"buy5price":16.89,"sell1shou":128390.0,"sell2shou":345600.0,"sell3shou":228850.0,"sell4shou":97820.0,"sell5shou":173990.0,"sell1price":16.94,"sell2price":16.95,"sell3price":16.96,"sell4price":16.97,"sell5price":16.98,"dealSumShou":4.1214547E7,"dealnowShou":0.0,"refclose":16.68,"batchId":"2016-11-231","code":"600000","name":"浦发银行","open":16.68,"high":17.23,"low":16.66,"close":16.94,"volume":0.0,"adj":0.0,"zhangdie":0.26000000000000156,"zhangdiefudu":1.5587529976019279,"dealmoney":7.01023113E8,"huanshoulv":0.0}
    val origintext = sc.textFile(originRtFile).filter(_.contains("{"))
    val originStockRealTimeDataRdd = origintext.map { x =>
      val stockRealTimeData: StockRealTimeData = HbaseService.convertMessage(x)
      stockRealTimeData
    }.filter {
      _.getClose > 0
    }
    val code2RtRdd = originStockRealTimeDataRdd.map { x => (x.getCode, x) }.groupByKey()

    //    获取大盘的batch  close
    val zsBatchId2DiffAndBatchIdList = code2RtRdd.filter(_._1.equals(zsCode)).map { x =>
      var batchId2closeMap = scala.collection.mutable.Map[String, BigDecimal]()
      var batchIdList = scala.collection.mutable.ArrayBuffer[String]()
      for (st <- x._2) {
        val nowBatchId = st.getBatchId.replace(date, "")
        batchId2closeMap += (nowBatchId -> new BigDecimal(st.getClose))
        batchIdList += (nowBatchId)
      }
      var batchId2DiffMap = scala.collection.mutable.Map[String, BigDecimal]()
      var tmpList = scala.collection.mutable.ArrayBuffer[String]()
      for (st <- x._2) {
        val nowBatchId = st.getBatchId.replace(date, "")
        val lastBatchId = Integer.parseInt(nowBatchId) - 1
        tmpList += st.getBatchId
        if (batchId2closeMap.contains(String.valueOf(lastBatchId))) {
          val lastClose = batchId2closeMap(String.valueOf(lastBatchId))
          val diff = new BigDecimal(st.getClose).divide(lastClose, 4, BigDecimal.ROUND_HALF_UP);
          batchId2DiffMap += (nowBatchId -> diff)
        }
      }
      (batchId2DiffMap, batchIdList)
    }.collect()
    //
    val zsBatchId2DiffBroadCast = sc.broadcast(zsBatchId2DiffAndBatchIdList(0)._1)
    //
    val batchIdList = zsBatchId2DiffAndBatchIdList(0)._2

    //
    //要获得每一次batch 变动的涨跌幅度，相对于大盘的幅度
    val batchId2ZsDiff: RDD[(String, scala.collection.mutable.Map[String, String])] = code2RtRdd.map { x =>
      val code = x._1
      var batchId2closeMap = scala.collection.mutable.Map[String, BigDecimal]()
      for (st <- x._2) {
        val nowBatchId = st.getBatchId.replace(date, "")
        batchId2closeMap += (nowBatchId -> new BigDecimal(st.getClose))
      }
      var batchId2ZsDiffMap = scala.collection.mutable.Map[String, String]()
      val zsBatchId2DiffMap = zsBatchId2DiffBroadCast.value
      for (st <- x._2) {
        val nowBatchId = st.getBatchId.replace(date, "")
        val lastBatchId = Integer.parseInt(nowBatchId) - 1
        if (batchId2closeMap.contains(String.valueOf(lastBatchId))) {
          val lastClose = batchId2closeMap(String.valueOf(lastBatchId))
          val diff = new BigDecimal(st.getClose).divide(lastClose, 4, BigDecimal.ROUND_HALF_UP);
          if (zsBatchId2DiffMap.contains(nowBatchId)) {
            val zsdiff = zsBatchId2DiffMap(nowBatchId)
            val diffWithZs = diff.divide(zsdiff, 4, BigDecimal.ROUND_HALF_UP).subtract(new BigDecimal(1))
            val close = st.getClose
            val closeString = new BigDecimal(close).setScale(4, BigDecimal.ROUND_HALF_UP).toString
            val clode_diffWithZs = diffWithZs.toString
            batchId2ZsDiffMap += (nowBatchId -> clode_diffWithZs)
          }
        }
      }
      (code, batchId2ZsDiffMap)

    }
    //获取基础的code  batch  rdd
    val code2batchId = originStockRealTimeDataRdd.map { rt =>
      val code = rt.getCode
      val close = new BigDecimal(rt.getClose).setScale(4, BigDecimal.ROUND_HALF_UP).toString
      val batchId: java.lang.Integer = java.lang.Integer.parseInt(rt.getBatchId.replace(date, ""))
      val batch2closeString = batchId + "_" + close
      (code, batch2closeString)
    }

    val code2batchIdWithTodayClose = code2batchId.join(code2TodyCloseRdd).map { x =>
      val code = x._1
      val todayClose = x._2._2
      var batch2closeString = x._2._1
      val currentClose = getBigDecimalSacle4(batch2closeString.split("_")(1))
      val didffWithTodayClose = currentClose.divide(todayClose, 4, BigDecimal.ROUND_HALF_UP).subtract(new BigDecimal(1)).toString
      (code, batch2closeString + "_" + todayClose.toString + "_" + didffWithTodayClose)


    }

    //要变成  code  batch Id  close   当前 batch diff2close   ... 前100  batch diff2close

    val filterNo100BatchRdd = code2batchIdWithTodayClose.join(batchId2ZsDiff).filter { x =>
      var returnResult: Boolean = true
      val code = x._1
      var batch2closeString = x._2._1
      val currentBatchId = java.lang.Integer.parseInt(batch2closeString.split("_")(0))
      val batchId100 = currentBatchId - batchNum
      var batchId2ZsDiffMap = x._2._2
      if (batchId2ZsDiffMap.contains(String.valueOf(batchId100))) {
        returnResult = true
      } else {
        returnResult = false
      }

      returnResult
    }
    //    //(300342,currentBatchId:24867	currentClose:14.0800	batchId0:0.0001	batchId1:-0.0001	batchId2:0.0001	batchId3:0.0000	batchId4:0.0000

    val interStringRdd = filterNo100BatchRdd.map { x =>
      var returnResult: Boolean = true
      val code = x._1
      var batch2closeString = x._2._1
      val currentBatchId = java.lang.Integer.parseInt(batch2closeString.split("_")(0))
      val currentClose = getBigDecimalSacle4(batch2closeString.split("_")(1))
      val todayClose = getBigDecimalSacle4(batch2closeString.split("_")(2))
      val diffWithToday = getBigDecimalSacle4(batch2closeString.split("_")(3))
      val label = getLabel(getBigDecimalSacle4(diffWithToday))
      var batchId2ZsDiffMap = x._2._2
      var integerString = ""
      var integerStringNoSchema = ""
      for (i <- 0 to batchNum - 1) {
        val batchIdbefore = currentBatchId - i
        if (i == 0) {
          integerString = "code:" + code + "\t" +
            "currentBatchId:" + currentBatchId + "\t" +
            "currentClose:" + currentClose + "\t" +
            "todayClose:" + todayClose + "\t" +
            "diffWithToday:" + diffWithToday + "\t" +
            "label:" + label + "\t" +
            "batchId" + i + ":" + batchId2ZsDiffMap(String.valueOf(batchIdbefore))
        } else {
          integerString = integerString + "\t" + "batchId" + i + ":" + batchId2ZsDiffMap(String.valueOf(batchIdbefore))
        }

      }
      for (i <- 0 to batchNum - 1) {
        val batchIdbefore = currentBatchId - i
        if (i == 0) {
          integerStringNoSchema = code + "\t" +
            currentBatchId + "\t" +
            currentClose + "\t" +
            todayClose + "\t" +
            diffWithToday + "\t" +
            label + "\t" +
            batchId2ZsDiffMap(String.valueOf(batchIdbefore))
        } else {
          integerStringNoSchema = integerStringNoSchema + "\t" + batchId2ZsDiffMap(String.valueOf(batchIdbefore))
        }

      }
      (code, integerString,integerStringNoSchema,label)
    }

    val ineterStringNoSchema = interStringRdd.map(_._3)
    val ineterString = interStringRdd.map(_._2)

//    FileUtils.forceDeleteOnExit(new File("/tmp/spark/preparert/"))

    val equaalLableRdd:RDD[String] =interStringRdd.map{x=>(x._4,x._3)}.groupByKey.flatMap{x=>
      val label = x._1
      var i=0
      var list = scala.collection.mutable.ArrayBuffer[String]()
      x._2.foreach{f=>
        i=i+1

        if(i<500){
          list += f
        }
      }
      list
    }

//    myPrint(equaalLableRdd,5)

    equaalLableRdd.repartition(1).saveAsTextFile("file:///tmp/spark/preparert/")

  }

  def getLastBatchId(batchId: String): String = {

    val lastBatchIDTmp = Integer.valueOf(batchId) - 1
    val lastBatchId = String.valueOf(lastBatchIDTmp)
    lastBatchId
  }


  def myPrint(rdd: RDD[_], num: Int): Unit = {
    rdd.take(num).foreach(println)
  }


  def getStockBaseRdd(sc: SparkContext, date: String): RDD[(String, BigDecimal)] = {

    val map1 = Map("url" -> "jdbc:mysql://db_mysql:3306/kms?user=root&password=", "dbtable" -> "base_stock", "driver" -> "com.mysql.jdbc.Driver")
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlcontext.read.format("jdbc").options(map1).load()
    df.registerTempTable("base_stock")
    val sql = "select code ,close from base_stock where date =" + "'" + date + "'"
    val baseStockRdd = sqlcontext.sql(sql)
    val codeTodyCloseRdd: RDD[(String, BigDecimal)] = baseStockRdd.map { baseStock =>
      val code = baseStock.getString(0)
      val todayClose: BigDecimal = getBigDecimalSacle4(baseStock.get(1))
      (code, todayClose)
    }
    codeTodyCloseRdd


  }


  def getBigDecimalSacle4(obj: Any): BigDecimal = {
    val returnValue = new BigDecimal(String.valueOf(obj)).setScale(4, BigDecimal.ROUND_HALF_UP)
    returnValue
  }

  def getBigDecimalSacle4ByDecimal(obj: BigDecimal): BigDecimal = {
    val returnValue = obj.setScale(4, BigDecimal.ROUND_HALF_UP)
    returnValue
  }

  def getLabel(diffWithToday: BigDecimal): java.lang.Integer = {
    var label = 1;
    var diffWithTodayDouble: java.lang.Double = diffWithToday.doubleValue()
    diffWithTodayDouble = diffWithTodayDouble * 100
    label = diffWithTodayDouble match {
      case a if diffWithTodayDouble <= -8 => 0
      case a if (diffWithTodayDouble <= -6 && diffWithTodayDouble > -8) => 1
      case a if (diffWithTodayDouble <= -4 && diffWithTodayDouble > -6) => 2
      case a if (diffWithTodayDouble <= -2 && diffWithTodayDouble > -4) => 3
      case a if (diffWithTodayDouble <= -0 && diffWithTodayDouble > -2) => 4
      case a if (diffWithTodayDouble <= 2 && diffWithTodayDouble > 0) => 5
      case a if (diffWithTodayDouble <= 4 && diffWithTodayDouble > 2) => 6
      case a if (diffWithTodayDouble <= 6 && diffWithTodayDouble > 4) => 7
      case a if (diffWithTodayDouble <= 8 && diffWithTodayDouble > 6) => 8
      case a if (diffWithTodayDouble > 8) => 9
    }

    label

  }

}

case class CodeBatchSum(code: String, batchId: String, diffSum: BigDecimal)


