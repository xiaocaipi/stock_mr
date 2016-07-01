package com.stock.kafka

import org.apache.spark.streaming.kafka.KafkaCluster
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaCluster

object test {
  
   val brokerAddress = "http://www.iteblog.com:9092"
//
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> brokerAddress,
        "group.id" -> "iteblog")

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("Test")
        sparkConf.set("spark.kryo.registrator", "utils.CpcKryoSerializer")
        val sc = new SparkContext(sparkConf)

        val ssc = new StreamingContext(sc, Seconds(2))
        val topicsSet = Set("iteblog")

        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
        messages.foreachRDD(rdd => {
            val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val kc = new KafkaCluster(kafkaParams) 
            for (offsets <- offsetsList) {
                val topicAndPartition = TopicAndPartition("iteblog", offsets.partition)
                val o = kc.setConsumerOffsets(args(0), Map((topicAndPartition, offsets.untilOffset)))
                if (o.isLeft) {
                    println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
                }
            }
        })

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
  
}