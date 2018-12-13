package com.stock.etl

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Base2Hive {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Base2Hive")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.max.dynamic.partitions", "100000")


    val startDate = args(0)
    val endDate = args(1)

    val sql = "(select * from base_stock where date >='"+startDate+"' and date <'"+endDate+"') as T"


    var jdbcDf=hiveContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://rm-uf6tvs4gz45fj5mk13o.mysql.rds.aliyuncs.com:3306/test?useUnicode=true&amp;characterEncoding=UTF-8",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable"-> sql,
      "numPartitions"-> "2",
      "user"->"root","password"->"Aa123456")).load()


    write2HdfsViaHive(hiveContext,jdbcDf)

  }

  def write2HdfsViaHive(sqlContext: HiveContext, df:DataFrame) = {

    //需要将df 注册成 临时表
    val tmpLogTable = "tmp_base"
    sqlContext.sql("use ods")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    df.repartition(4).registerTempTable(tmpLogTable)



    val insertSQL =
      s"""
         |insert into ods.base_stock
         |select *
         |from $tmpLogTable
       """.stripMargin
    sqlContext.sql(insertSQL)
  }

}
