package com.stock

import java.math.BigDecimal

import com.stock.util.{HbaseClientUtil, StockCommonUtil}
import com.stock.vo.StockAlertVo
import org.apache.commons.io.FileUtils
import java.io.File
import java.util

import org.apache.spark.{SparkConf, SparkContext}

object app {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GetBigDeal").setMaster("local")
    conf.set("spark.default.parallelism", "20")
    val sc = new SparkContext(conf)
    var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    println (rdd1.partitions.length)
    val rdd2  = rdd1.reduceByKey(_+_,1)
    println (rdd2.partitions.length)
    rdd2.foreach(println)
    Thread.sleep(100000)

    println(111)

    //    var states = scala.collection.mutable.Map[String, String]()
    //    println (states.get("aa") == None)
    //    val aa1 = new BigDecimal(12)
    //    val aa2 = new BigDecimal(13)
    //    println(aa1.compareTo(aa2))

    //     val alertVo = new StockAlertVo()
    //    alertVo.setCode("000723")
    //    alertVo.setStatus("1")
    //    HbaseClientUtil.insertByObject(alertVo, "test_alert", "base_cf")

//    val aa = "13.01_13:11:42|13.01_13:11:48".split("\\|")
//    val firstClose: String = aa(0).split("_")(0)
//    println(firstClose)

//    val i = 100;
//    for (j <- -11 to 11) {
//      val a=getLabel(new BigDecimal(j).divide(new BigDecimal(100)))
//      println(a)
//    }
//    FileUtils.forceDeleteOnExit(new File("/tmp/spark/preparert/"))

//    print(StockCommonUtil.getBatchId("2017-10-09117652"))

//    val i =0
//    for( j<-1 to 10){
//      println (j)
//    }

  }

  def getLabel(diffWithToday:BigDecimal):java.lang.Integer={
    var label =1;
    var diffWithTodayDouble:java.lang.Double = diffWithToday.doubleValue()
    diffWithTodayDouble = diffWithTodayDouble*100
    label = diffWithTodayDouble match {
      case a if diffWithTodayDouble <= -8 =>  -5
      case a if (diffWithTodayDouble <= -6 && diffWithTodayDouble > -8) => -4
      case a if (diffWithTodayDouble <= -4 && diffWithTodayDouble > -6) => -3
      case a if (diffWithTodayDouble <= -2 && diffWithTodayDouble > -4) => -2
      case a if (diffWithTodayDouble <= -0 && diffWithTodayDouble > -2) => -1
      case a if (diffWithTodayDouble <=  2 && diffWithTodayDouble >  0) => 1
      case a if (diffWithTodayDouble <=  4 && diffWithTodayDouble >  2) => 2
      case a if (diffWithTodayDouble <=  6 && diffWithTodayDouble >  4) => 3
      case a if (diffWithTodayDouble <=  8 && diffWithTodayDouble >  6) => 4
      case a if ( diffWithTodayDouble >  8) => 5
    }

    label

  }

}