package com.sparkkafka.uber


import java.util.{ Map => JMap }

import org.apache.kafka.common.serialization.Serializer
import org.apache.spark.streaming.kafka.producer._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.{ ConstantInputDStream, DStream }

class ItemJsonSerializer extends Serializer[Item] {
  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = { /* NOP */ }

  override def serialize(topic: String, data: Item): Array[Byte] = data.toString.getBytes

  override def close(): Unit = { /* NOP */ }
}

case class Item(id: Int, value: Int) {
  override def toString: String = s"""{"id":"$id","value":"$value"}"""
}

/**
 * Produces messages to Kafka.
 * Usage: KafkaProducerExample <kafkaBrokers> <topics> <numMessages>
 *   <kafkaBrokers> is a list of one or more kafka brokers
 *   <topics> is a list of one or more kafka topics
 *   <numMessages> is the number of messages that the kafka producer should send
 * 
 * Use Apache Spark Version 2.2.0, as 2.3.1 is having some conflicting Jar Files. 
 *      
 */

// scalastyle:off println
object KafkaProducerExample extends App {
  

  val Array(topics) = args
  val kafkaBrokers = "localhost:9092"
  val numMessages = 10

  val batchTime = Seconds(2)

  val sparkConf = new SparkConf()
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .setAppName(getClass.getCanonicalName)
    .setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, batchTime)

  val producerConf = new ProducerConf(
    bootstrapServers = kafkaBrokers.split(",").toList
  )

  val items = (0 until numMessages.toInt).map(i => Item(i, i))
  val defaultRDD: RDD[Item] = ssc.sparkContext.parallelize(items)
  val dStream: DStream[Item] = new ConstantInputDStream[Item](ssc, defaultRDD)
  dStream.sendToKafka[ItemJsonSerializer](topics, producerConf)
  dStream.count().print()

  ssc.start()
  ssc.awaitTermination()
  

  ssc.stop(stopSparkContext = true, stopGracefully = true)
}

