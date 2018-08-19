package com.redhat.gpte

import org.apache.spark.sql.Row
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf


object DataCreation {
  
   def main(args: Array[String]) {
      val conf=new SparkConf().setAppName("Sample CSV").setMaster("local")
      val sc=new SparkContext(conf)
      val sqlContext=new SQLContext(sc)
      
      val pathToFile="src/main/resources/data/Uber-Jan-Feb-FOIL.csv"
      
      val df=sqlContext.
             read.
             format("com.databricks.spark.csv").
             option("header","true").
             option("inferschema","true").
             load(pathToFile)

      print("starting >>>>>>>>")
      
      df.rdd.cache()
      df.rdd.foreach(println)
      print(df.printSchema())
      df.registerTempTable("UberData")
      
      sqlContext.sql("Select * from UberData").collect().foreach(println)
      
      print("Ending >>>>>>>>>>>")
}  
}
