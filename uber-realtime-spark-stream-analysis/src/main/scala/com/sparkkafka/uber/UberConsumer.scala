
package com.sparkkafka.uber

import consumer.kafka.{ProcessedOffsetManager, ReceiverLauncher}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext};

object KafkaConsumer {

  def main(arg: Array[String]): Unit = {

//    import org.apache.log4j.{Level, Logger}
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)

    //Create SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaConsumer")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "UberTopic"
    val zkhosts = "uber-zookeeper-host"
    val zkports = "2181"

    //Specify number of Receivers you need. 
    val numberOfReceivers = 1

    val kafkaProperties: Map[String, String] = 
	Map("zookeeper.hosts" -> zkhosts,
        "zookeeper.port" -> zkports,
        "kafka.topic" -> topic,
        "zookeeper.consumer.connection" -> "uber-zookeeper-host:2181",
        "kafka.consumer.id" -> "kafka-consumer",
        "bootstrap.servers" -> "uber-bootstrap-servers:9092",
        //optional properties
        "consumer.forcefromstart" -> "true",
        "consumer.backpressure.enabled" -> "true",
        "consumer.fetchsizebytes" -> "1048576",
        "consumer.fillfreqms" -> "1000",
        "consumer.num_fetch_to_buffer" -> "1")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}

    val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers,StorageLevel.MEMORY_ONLY)
    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(tmp_stream, props)

    //Start Application Logic
    tmp_stream.foreachRDD(rdd => {
        println("\n\nNumber of records in this batch : " + rdd.count())
    } )
    //End Application Logic

    //Persists the Max Offset of given Kafka Partition to ZK
    ProcessedOffsetManager.persists(partitonOffset_stream, props)
    ssc.start()
    ssc.awaitTermination()


  }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable
  case class Center(cid: Integer, clat: Double, clon: Double) extends Serializable
  val schema = StructType(Array(
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))

  def parseUber(str: String): Uber = {
    val p = str.split(",")
    Uber(p(0), p(1).toDouble, p(2).toDouble, p(3))
  }

}

