package com.citi.training.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object FileStreamTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test-sparkstream-app")
    val streamingContext=new StreamingContext(conf,Seconds(30))
    val dStream=streamingContext.textFileStream("c:/teststreaming")
    dStream.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_).print()
    
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}