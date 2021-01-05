package com.citi.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark=SparkSession.builder().appName("emp-data-sql-app").master("local[*]").getOrCreate();
		Dataset<Row>empDF=spark.read().format("csv").option("header", "true").load("c:/testcsv3/employee.csv");
				    empDF.show();
				    spark.close();

	}

}
