package com.citi.training.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object First {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test-spark-app")
    
    val sc=new SparkContext(conf)
     val array=sc.textFile("c:/test/first.txt").map(line=>line.toUpperCase).filter(line=>line.startsWith("THIS")).collect
     array.foreach(println)
  }
}