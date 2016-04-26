package com.sourcelearning.le03

import com.stock.util.SparkConfUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Appstart {
  
  def main(args: Array[String]): Unit = {
    
    val WAIT_TIMEOUT_MILLIS = 1000000
     val jobCompletionTime = 1421191296660L
    
    val sparkConf = new SparkConf()
    
    sparkConf.setMaster("local")
    sparkConf.setAppName("test")
    
    val sc = new SparkContext(sparkConf)
    
    val liveListenBus = new LiveListenerBus()
    
    val counter = new BasicJobCounter
    
    liveListenBus.addListener(counter)
    
    (1 to 5).foreach { _ => liveListenBus.post(SparkListenerJobEnd(0, jobCompletionTime)) }
    
    liveListenBus.start(sc)
    
    liveListenBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
  }
  
  
  
}