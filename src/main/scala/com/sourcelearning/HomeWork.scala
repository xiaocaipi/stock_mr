package com.sourcelearning

import com.stock.util.SparkConfUtil

object HomeWork {
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = SparkConfUtil.getSparkConf(args,"text",true,false)
    val sc = SparkConfUtil.getSparkContext
    val path ="/user/root/spark_source/users.txt"
    val users_hdfs_file =  sc.textFile(path)
    val area_count = users_hdfs_file.map { x => x.split(",") }.map { x => (x(3),1) }.reduceByKey((x,y) => x + y)
    
    area_count.take(10).foreach(println)
    
    val num_3_count = users_hdfs_file.map { x => x.split(",") }.map ( x => x(2).subSequence(0, 3) ).map ( (_,1) ).reduceByKey((x,y) => x + y)
    num_3_count.take(10).foreach(println)
    
  }
}