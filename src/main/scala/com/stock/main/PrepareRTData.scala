package com.stock.main

import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.stock.hbase.HbaseService
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.storage.StorageLevel
import util.CommonUtil
import org.apache.spark.SparkContext

//    args(1)="hdfs://hadoop3:8020/s/data/origin_rt/"
object PrepareRTData {

  val logger = Logger.getRootLogger
  def main(args: Array[String]) {

    //	logger.setLevel(Level.ERROR)
    if (args.length < 2) {
      logger.error("param is not correct")
      System.exit(1)
    }
    val date = args(0)
    val originRtPath = args(1);
    val originRtFile = originRtPath + date

    val krbconfPath = CommonUtil.getPropertyValue("krbconfPath");
    val principal = CommonUtil.getPropertyValue("principal");
    val keytabPath = CommonUtil.getPropertyValue("keytabPath");
    val hbaseSitePath = CommonUtil.getPropertyValue("hbase.site.path");
    val hiveSitePath = CommonUtil.getPropertyValue("hive.site.path");
    val hdfsSitePah = CommonUtil.getPropertyValue("hdfs.site.path");

    val conf = new SparkConf().setAppName("PrepareRTData").setMaster("local")
    
    val sc = new SparkContext(conf)
    sc.addFile(hdfsSitePah)
    val origintext = sc.textFile(originRtFile)
    origintext.foreach(println(_))

  }

  def formatRtOrigin2HiveFile(origintext: String, date: String): String = {
    var stockRealTimeData = HbaseService.convertMessage(origintext)
    var returnString = date + "\t" + stockRealTimeData.getBuy1price()
    println(returnString)
    returnString
  }

}