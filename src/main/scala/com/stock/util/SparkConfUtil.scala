package com.stock.util;

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkContext, SparkConf }

/**
 * Created by caidanfeng733 on 12/16/15.
 */
object SparkConfUtil {

  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null
  var sqlContext: SQLContext = null
  var hiveContext: HiveContext = null

  def getSparkConf(args: Array[String]): SparkConf = {

    if (sparkConf == null) {
      sparkConf = new SparkConf()
      if (args == null || args.length == 0) {
        sparkConf
      } else {
        val principal = args(0)
        val keytabPath = args(1)
        val krbconfPath = args(2)

        sparkConf.set("hadoop.security.authentication", "kerberos")
        sparkConf.set("hbase.security.authentication", "kerberos")

        System.setProperty("java.security.krb5.conf", krbconfPath)
        sparkConf.set("spark.yarn.keytab", keytabPath)
        sparkConf.set("spark.yarn.principal", principal)
        sparkConf.set("spark.akka.timeout", "900")
        sparkConf.set("spark.akka.frameSize", "1024")

        sparkConf.setAppName("fraudGroup")

      }

    }
    sparkConf

  }

  def getSparkContext: SparkContext = {
    if (sparkContext == null) {
      sparkContext = new SparkContext(sparkConf)
    }
    sparkContext
  }

  def getsqlContext: SQLContext = {
    if (sqlContext == null) {
      sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    }
    sqlContext
  }

  def gethiveContext: HiveContext = {
    if (hiveContext == null) {
      hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    }
    hiveContext
  }

}