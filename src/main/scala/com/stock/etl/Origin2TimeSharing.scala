package com.stock.etl

import com.stock.util.SparkConfUtil
import com.stock.vo.StockRealTimeData
import com.stock.hbase.HbaseService
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import com.stock.udf.Second2MinuteUDF
import org.apache.spark.sql.types.DataTypes
import com.stock.sparkstreaming.SQLContextSingleton
import scala.collection.mutable.ListBuffer
import com.stock.vo.StockTimeSharingVo

object Origin2TimeSharing {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    if (args.length != 1) {
      Logger.getRootLogger.error("wrong param please input hdfs path ")
      return
    }


    val sparkConf = SparkConfUtil.getSparkConf(args, "Origin2TimeSharing", false, false)
    val sc = SparkConfUtil.getSparkContext
    
    val originPath = args(0)

    val hdfsFile = sc.textFile(originPath, 10);

    val sqlContext = SparkConfUtil.gethiveContext

    sqlContext.udf.register("s2m",
      new Second2MinuteUDF(), DataTypes.StringType);

    import sqlContext.implicits._

    sqlContext.read.json(hdfsFile).registerTempTable("stock_rt")

    var sql = """select t1.code, t1.minutes, t1.close, t1.dealSumShou
  from (select *,
               s2m(time) minutes,
               row_number() OVER(PARTITION BY code, s2m(time) ORDER BY time DESC) rn
          from stock_rt) t1
 where t1.rn = 1

"""
    //    sql = "select time,count(*) from stock_rt group by time "
    //    sql = "select distinct time from stock_rt  "

    val wordCountsDataFrame = sqlContext.sql(sql)

    wordCountsDataFrame.rdd.foreachPartition { partition =>
      var voList = new ListBuffer[StockTimeSharingVo]()
      var tmp = new ListBuffer[String]
      partition.foreach { x =>
        val vo = new StockTimeSharingVo();
        vo.setCode(x.getString(0))
        vo.setMinutes(x.getString(1))
        vo.setClose(x.getDouble(2))
        vo.setDealSumShou(x.getDouble(3))
        val rowkey = x.getString(0) + "_" + x.getString(1)
        vo.setRowkey(rowkey)

        tmp += x.getString(0)
        voList += vo
      }

      HbaseService.insertStockTimeSharing(voList)

    }

  }

}