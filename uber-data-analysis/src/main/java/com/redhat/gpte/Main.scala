package com.redhat.gpte

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans


object Main {
  
      //Just Like main method in Java
      def main(args: Array[String]) {
        
        // Initialize the spark Session
         val spark: SparkSession = SparkSession.builder().appName("uber").master("local[*]").getOrCreate()

    import spark.implicits._
 //Define your structured Schema
    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))
    
    // Using Latest Version of Spark 2.3.1
    val df: Dataset[Uber] = spark.read.option("inferSchema", "false").schema(schema).csv("src/main/resources/data/uber.csv").as[Uber]
    
        df.cache  //Cache the DataSet [uber] in memory (Action 1)
    df.show //Print them (Action 2) Please refer Actions and Transformations in Apache Spark
    df.schema //Arrange them in a structured schema (Action 3)
   // Constructing an Array using Latitude and Longitude
    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)

    // increase the iterations if running on a cluster, Optimum is 5 iterations.
    val kmeans = new KMeans().setK(20).setFeaturesCol("features").setMaxIter(5)
    val model = kmeans.fit(trainingData)
    println("Final Centers: ")
    //Model the ClusterCenters
    model.clusterCenters.foreach(println) 

    val categories = model.transform(testData) 

    categories.show
    //Register with and Store in a Temp Table named "uber", which helps in firing queries in Memory
    categories.createOrReplaceTempView("uber")

        
    // SparkSQL Queries -- For Multiple scenarios (Which is explained in Zeppelin Notebook and Visualized at every queries
    categories.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction").groupBy("month", "day", "hour", "prediction").agg(count("prediction").alias("count")).orderBy("day", "hour", "prediction").show

    categories.select(hour($"dt").alias("hour"), $"prediction").groupBy("hour", "prediction").agg(count("prediction")
      .alias("count")).orderBy(desc("count")).show

    categories.groupBy("prediction").count().show()

    spark.sql("select prediction, count(prediction) as count from uber group by prediction").show

    spark.sql("SELECT hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)").show


    val res = spark.sql("select dt, lat, lon, base, prediction as cid FROM uber order by dt")   
      res.show()
      //We can also write it to a JSON format, But I have commented here, Since It may use intensive resources.
     //res.write.format("json").save("com/redhat/gpte/data/uber.json")
  }
}

//Just like Domain Object in Java

case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable


 
