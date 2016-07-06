package com.stock.sparkstreaming

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{ Level, Logger }
import scala.collection.mutable.ListBuffer
import util.CommonUtil
import com.stock.vo.StockRealTimeData
import com.stock.service.StockRTService
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.math.BigDecimal
import com.stock.vo.StockRtDiffWithZSVo
import java.text.DecimalFormat
import org.apache.spark.sql.types.DataTypes
import java.util.Arrays
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

object StockRtStrong {

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.ERROR)

    val zkQuorum = CommonUtil.getPropertyValue("zkQuorum")
    val topics = CommonUtil.getPropertyValue("topics")
    val group = CommonUtil.getPropertyValue("group")
    val numThreads = CommonUtil.getPropertyValue("numThreads")
    val principal = CommonUtil.getPropertyValue("principal")
    val keytabPath = CommonUtil.getPropertyValue("keytabPath")
    val krbconfPath = CommonUtil.getPropertyValue("krbconfPath")

    val checkPointPath = CommonUtil.getPropertyValue("checkPointPath")

    val sparkConf = new SparkConf().setAppName("stockRt").setMaster("local[2]")

    sparkConf.set("spark.default.parallelism", "16")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    var alertMap = HbaseService.getStockAlert;
    var alertBroad = ssc.sparkContext.broadcast(alertMap)

    val topicMap = topics.split(",").map((_, 2.toInt)).toMap
    val numStreams = 4
    //        val kafkaStreams = (1 to numStreams).map { i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) }
    //        val lines = ssc.union(kafkaStreams)

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val codeclose = lines.map { line =>
      val stockRtDetail: StockRealTimeData = HbaseService.convertMessage(line)
      stockRtDetail
    }
      //    .filter { _.getClose !=0 }
      .map { stockRtDetail =>
        (stockRtDetail.getCode, stockRtDetail.getClose + "_" + stockRtDetail.getTime + "_" + stockRtDetail.getBatchId + "_" + stockRtDetail.getZhangdiefudu)
      }

    val codeCloseDiff = codeclose
      //            .filter(_._1.equals("000831"))
      .reduceByKeyAndWindow((a1: String, a2: String) => {
        a1 + "|" + a2
      }, Seconds(36), Seconds(3)).filter(_._2.split("\\|").length >= 10)
      .map { x =>
        val array = x._2.split("\\|")
        val len = array.length
        val firstClose: String = array(0).split("_")(0)
        val lastClose: String = array(len - 1).split("_")(0)
        val thisTime = array(len - 1).split("_")(1)
        val batch_id = array(len - 1).split("_")(2)
        val zhangdiefudu = array(len - 1).split("_")(3)
        val last3Close: String = array(len - 2).split("_")(0)
        val last6Close: String = array(len - 3).split("_")(0)
        val last9Close: String = array(len - 4).split("_")(0)
        val last21Close: String = array(len - 8).split("_")(0)
        val last30Close: String = array(len - 10).split("_")(0)

        val diff3 = new BigDecimal(lastClose)
        //        .subtract(new BigDecimal(last3Close))
        val diff3Percentage = diff3.divide(new BigDecimal(last3Close), 4, BigDecimal.ROUND_HALF_UP);

        val diff6 = new BigDecimal(lastClose)
        //        .subtract(new BigDecimal(last6Close))
        val diff6Percentage = diff6.divide(new BigDecimal(last6Close), 4, BigDecimal.ROUND_HALF_UP);

        val diff9 = new BigDecimal(lastClose)
        //        .subtract(new BigDecimal(last9Close))
        val diff9Percentage = diff9.divide(new BigDecimal(last9Close), 4, BigDecimal.ROUND_HALF_UP);

        val diff21 = new BigDecimal(lastClose)
        //        .subtract(new BigDecimal(last21Close))
        val diff21Percentage = diff21.divide(new BigDecimal(last21Close), 4, BigDecimal.ROUND_HALF_UP);

        val diff30 = new BigDecimal(lastClose)
        //        .subtract(new BigDecimal(last30Close))
        val diff30Percentage = diff30.divide(new BigDecimal(last30Close), 4, BigDecimal.ROUND_HALF_UP);

        StreamingStockRtDto(x._1, thisTime, batch_id, new BigDecimal(zhangdiefudu),
          diff3Percentage,
          diff6Percentage,
          diff9Percentage,
          diff21Percentage,
          diff30Percentage)

      }

    //caculate the diff with 399006
    val zs2Diff = codeCloseDiff.filter(_.code.equals("399006")).map { x => ("399006", x) }

    val commonDiff = codeCloseDiff.map { x => ("399006", x) }

    val joinDiffDStream = commonDiff.join(zs2Diff).map { x =>
      val common = x._2._1
      val zs2 = x._2._2

      var diff_sum_3 = common.diff_3_s.divide(zs2.diff_3_s, 4, BigDecimal.ROUND_HALF_UP).doubleValue() - 1
      var diff_sum_6 = common.diff_6_s.divide(zs2.diff_6_s, 4, BigDecimal.ROUND_HALF_UP).doubleValue() - 1
      var diff_sum_9 = common.diff_9_s.divide(zs2.diff_9_s, 4, BigDecimal.ROUND_HALF_UP).doubleValue() - 1
      var diff_sum_21 = common.diff_21_s.divide(zs2.diff_21_s, 4, BigDecimal.ROUND_HALF_UP).doubleValue() - 1
      var diff_sum_30 = common.diff_30_s.divide(zs2.diff_30_s, 4, BigDecimal.ROUND_HALF_UP).doubleValue() - 1

      var vo = new StockRtDiffWithZSVo(common.code, common.time, common.batch_id,
        diff_sum_3,
        diff_sum_6,
        diff_sum_9,
        diff_sum_21,
        diff_sum_30)
      vo.setRowkey(common.code + "--" + common.batch_id)
      vo.setZhangdiefudu(common.zhangdiefudu.doubleValue())
      (common.code, vo)
    }

//        joinDiffDStream.filter(_._1.equals("000831")).print()

    var sumStockRtDiffWithZSVo = (one: StockRtDiffWithZSVo, two: StockRtDiffWithZSVo) => {
      if (one != null) {
        one.setDiff_sum_3(one.getDiff_sum_3 + two.getDiff_sum_3)
        one.setDiff_sum_6(one.getDiff_sum_6 + two.getDiff_sum_6)
        one.setDiff_sum_9(one.getDiff_sum_9 + two.getDiff_sum_9)
        one.setDiff_sum_21(one.getDiff_sum_21 + two.getDiff_sum_21)
        one.setDiff_sum_30(one.getDiff_sum_30 + two.getDiff_sum_30)
        one.setTime(two.getTime)
        one.setBatch_id(two.getBatch_id)
        one.setRowkey(two.getRowkey)
        one.setZhangdiefudu(two.getZhangdiefudu)
      }

      one
    }

    val updateFunc = (newValues: Seq[StockRtDiffWithZSVo], previouState: Option[StockRtDiffWithZSVo]) => {
      var current: StockRtDiffWithZSVo = null
      if (newValues.size > 0) {
        for (stockRtDiffWithZSVo <- newValues) {
          if (current == null) {
            current = stockRtDiffWithZSVo
          } else {
            sumStockRtDiffWithZSVo(current, stockRtDiffWithZSVo)
          }
        }
      }

      val previous = previouState.getOrElse(current)
      if (current == null) {
        Some(previous)
      } else {
        Some(sumStockRtDiffWithZSVo(previous, current))
      }

    }

    ssc.checkpoint(checkPointPath)

    val updateStateDstream = joinDiffDStream.updateStateByKey[StockRtDiffWithZSVo](updateFunc)

        updateStateDstream.filter(_._1.equals("399006")).print()

    val sortRdd = updateStateDstream.transform { rdd =>
      val rowRdd = rdd.map { x =>
        val vo = x._2
        RowFactory.create(vo.getCode, vo.getDiff_sum_3, vo.getZhangdiefudu)
      }

      val schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("code", DataTypes.StringType, true),
        DataTypes.createStructField("diff_sum_3", DataTypes.DoubleType, true),
        DataTypes.createStructField("zhangdiefudu", DataTypes.DoubleType, true)));

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext);

      val df = sqlContext.createDataFrame(rowRdd, schema);

      df.registerTempTable("stock_rt_diff")

      val returnDf = sqlContext.sql("select * from stock_rt_diff where zhangdiefudu<3 and code != '000001' order by diff_sum_3 desc limit 3 ")

      returnDf.rdd

    }

//    sortRdd.print()

    //    val shoudSortDstream = updateStateDstream
    //    .filter(_._2.getZhangdiefudu<3)
    //    .map{x=>
    //      val stockRtDiffWithZsVo = x._2
    //      (stockRtDiffWithZsVo.getDiff_sum_3,stockRtDiffWithZsVo)  
    //    }

    //    joinDiffDStream.foreachRDD((rdds: RDD[StockRtDiffWithZSVo]) => {
    //
    //      rdds.foreachPartition(partitionOfRecords => {
    //         var stockRtDiffWithZsList = new ListBuffer[StockRtDiffWithZSVo]()
    //        partitionOfRecords.foreach(stockRtDiffWithZSVo => {
    //            stockRtDiffWithZsList += stockRtDiffWithZSVo
    //        })
    //      HbaseService.insertStockRTDiffWithZS(stockRtDiffWithZsList)
    //
    //      })
    //
    //    })

    //    lines.foreachRDD((rdds: RDD[String])=> {
    //      
    //
    //      val sqlContext = SQLContext.getOrCreate(rdds.sparkContext)
    //      import sqlContext.implicits._
    //
    //      val wordsDataFrame = sqlContext.read.json(rdds)
    //
    //      // Register as table
    //      wordsDataFrame.registerTempTable("stock_rt")
    //
    //      val wordCountsDataFrame =
    //        sqlContext.sql("select time,count(*) count from stock_rt group by time")
    //      wordCountsDataFrame.show()
    ////      wordsDataFrame.printSchema()
    //
    //    })

    ssc.start()
    ssc.awaitTermination()
  }

  def windowsMethod(f: Double => Double): (Double, Double) => Double = (x: Double, y: Double) => {
    if (f(x) == 0) {
      0
    } else {
      new BigDecimal(f(x) + f(y)).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue()
    }

  }

}
case class Record(word: String)

case class codePrice(code: String, price: BigDecimal)

case class StreamingStockRtDto(code: String,
  time: String,
  batch_id: String,
  zhangdiefudu: BigDecimal,
  diff_3_s: BigDecimal,
  diff_6_s: BigDecimal,
  diff_9_s: BigDecimal,
  diff_21_s: BigDecimal,
  diff_30_s: BigDecimal)

object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}