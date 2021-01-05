package com.citi.training.spark.sql

import org.apache.spark.sql.SparkSession

object EmployeeDataTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("emp-data-sql-app").master("local[*]").getOrCreate()
    val empDF=spark.read.format("csv").option("header", "true").load("c:/testcsv3/employee.csv")
    empDF.show()
    spark.close()
  }
}