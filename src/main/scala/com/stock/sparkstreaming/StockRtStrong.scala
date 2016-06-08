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
//    lines.print()
    
    val codeclose = lines.map (HbaseService.convertMessage(_)).map( x=>(x.getCode,new BigDecimal(x.getClose)))
    codeclose.filter(_._1.equals("000831")).reduceByKeyAndWindow((a:BigDecimal,b:BigDecimal) => (a.divide(b)), Seconds(3), Seconds(3)).print()
    
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

}
case class Record(word: String)

case class codePrice(code:String,price:BigDecimal)

object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}