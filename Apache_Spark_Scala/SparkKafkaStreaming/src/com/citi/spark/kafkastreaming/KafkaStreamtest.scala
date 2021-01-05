package com.citi.spark.kafkastreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder

object KafkaStreamTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test-sparkstream-app")
    val streamingContext=new StreamingContext(conf,Seconds(60))
    val  kafkaStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](streamingContext,
	Map("metadata.broker.list"->"localhost:9092"),Set("test-topic"))
    kafkaStream.map(msg=>msg._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}