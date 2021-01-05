package com.citi.training.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    //conf.setMaster("local[*]")
    conf.setAppName("wordcount-spark-app")
    
    val sc=new SparkContext(conf)
    sc.textFile("c:/test/words.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(t=>t._2,false).collect.foreach(println)
    
  }
}