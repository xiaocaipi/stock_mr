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

object Origin2TimeSharing {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    val sparkConf = SparkConfUtil.getSparkConf(args, "Origin2TimeSharing", true)
    val sc = SparkConfUtil.getSparkContext

    val originPath = "hdfs://cdh1:8020/user/root/stock/rt/2016-07-01_bak"

    val hdfsFile = sc.textFile(originPath, 10);

    val sqlContext = SparkConfUtil.gethiveContext

    sqlContext.udf.register("s2m",
      new Second2MinuteUDF(), DataTypes.StringType);

    import sqlContext.implicits._

    sqlContext.read.json(hdfsFile).registerTempTable("stock_rt")

    var sql = """select *,
       row_number() OVER(PARTITION BY code, s2m(time) ORDER BY time DESC) rank
  from stock_rt 
"""
    //    sql = "select time,count(*) from stock_rt group by time "
    //    sql = "select distinct time from stock_rt  "

    val wordCountsDataFrame = sqlContext.sql(sql)

    wordCountsDataFrame.rdd.collect().foreach { println }

  }

}