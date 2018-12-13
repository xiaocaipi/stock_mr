package com.stock.main

import java.math.BigDecimal

import com.stock.util.{SortUtil, StockCommonUtil}
import com.stock.vo.{StockData, StockRealTimeData}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.DateUtil
import util.CommonUtil
import java.util
import util.{Arrays, Collections, Properties}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConversions._

//    args(1)="hdfs://hadoop3:8020/s/data/origin_rt/"
object mysql2hive {

  val logger = Logger.getRootLogger

  def main(args: Array[String]) {

    if (args.length != 4) {
      return
    }
    var tableName = args(0)
    val isShoudCreateTable = args(1)
    //判断是否权量同步，是的话 就全部同步，不是的话，就同步当天的
    val isGolbal = args(2)

    val date =args(3)
    val conf = new SparkConf().setAppName("mysql2hive").setMaster("local[2]")
//        val conf = new SparkConf().setAppName("mysql2hive")
    conf.set("spark.default.parallelism", "10")
    conf.set("spark.yarn.executor.memoryOverhead", "1024")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val thisDate = date

    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path("hdfs://cdh:8020/user/root/stock/"+tableName+"/time="+thisDate)
    if(hdfs.exists(path)){
      hdfs.delete(path,true)
    }


    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._
    val tableSchemaArrayBuffer = getTableSchema(sc, tableName)
    if (isShoudCreateTable.equals("1")) {

      createTable(hiveContext, tableSchemaArrayBuffer, tableName)
    }

    val mysqlTuple = getMysqlDataRdd(sqlContext, tableName)
    val sqlRdd = mysqlTuple._2
    val sqlSchema = mysqlTuple._1

    var querySql = "insert into table " + tableName + " partition(time='" + thisDate + "')  select "
    tableSchemaArrayBuffer.foreach { x =>
      val field = x._1
      querySql = querySql + " " + field + " ,"
    }
    querySql = querySql.substring(0, querySql.length - 1)
    querySql = querySql + " from " + tableName + "_tmp where 1=1"
    if (!isGolbal.equals("1")) {
      querySql = querySql + " and date ='" + thisDate + "' "
    }
    hiveContext.createDataFrame(sqlRdd, sqlSchema).registerTempTable(tableName + "_tmp")
    hiveContext.sql("use stock")
    hiveContext.sql("alter table " + tableName + " drop partition (time='" + thisDate + "')")
    hiveContext.sql(querySql)


  }


  def getTableSchema(sc: SparkContext, tableName: String): scala.collection.mutable.ArrayBuffer[(String, String)] = {

    val map1 = Map("url" -> "jdbc:mysql://db_mysql:3306/INFORMATION_SCHEMA?user=root&password=", "dbtable" -> "COLUMNS", "driver" -> "com.mysql.jdbc.Driver")
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlcontext.read.format("jdbc").options(map1).load()
    df.registerTempTable("COLUMNS")
    val sql = "SELECT COLUMN_NAME,DATA_TYPE FROM COLUMNS  where TABLE_SCHEMA='kms'  and TABLE_NAME ='base_stock'"
    val baseStockRdd = sqlcontext.sql(sql)
    val schemaArrayBuffer = scala.collection.mutable.ArrayBuffer[(String, String)]()
    val codeTodyStockDataRdd: RDD[(String, String)] = baseStockRdd.map { baseStockSchema =>
      val field = baseStockSchema.getString(0)
      val dataTypes = baseStockSchema.getString(1)
      (field, dataTypes)
    }
    codeTodyStockDataRdd.collect().foreach { x =>
      val tuple = (x._1, x._2)
      schemaArrayBuffer += tuple
    }
    schemaArrayBuffer

  }

  def getMysqlDataRdd(sqlcontext: SQLContext, tableName: String): (StructType, RDD[Row]) = {

    val map1 = Map("url" -> "jdbc:mysql://db_mysql:3306/kms?user=root&password=", "dbtable" -> tableName, "driver" -> "com.mysql.jdbc.Driver")

    val df = sqlcontext.read.format("jdbc").options(map1).load()
    val sqlSchema = df.schema
    val sqlRdd = df.rdd
    (sqlSchema, sqlRdd)


  }


  def createTable(sqlContext: HiveContext, tableSchemaArrayBuffer: scala.collection.mutable.ArrayBuffer[(String, String)], tableName: String) = {
    sqlContext.sql("use stock")


    var createSql = "create EXTERNAL table " + tableName + " ( "
    tableSchemaArrayBuffer.foreach { x =>
      val field = x._1
      val dataTypes = x._2
      if (!dataTypes.equals("varchar")) {
        createSql = createSql + " `" + field + "` decimal(38,15),"
      } else {
        createSql = createSql + " `" + field + "` string,"
      }

    }
    createSql = createSql.substring(0, createSql.length - 1)
    createSql = createSql + ") partitioned by (time string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE location '/user/root/stock/" + tableName + "/'"
    sqlContext.sql(createSql)
  }

}



