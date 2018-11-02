package com.sparkkafka.uber

import java.io.BufferedReader

import java.io.File

import java.io.FileReader

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.kafka.clients.producer.ProducerRecord

//remove if not needed
//import scala.collection.JavaConversions._

object UberRawDataProducer {
  
 // Declare a new producer
     val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("client.id", "uberrawdata")
    props.put("acks", "all")
    props.put("retries", new Integer(1))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(133554432))
   

    val producer = new KafkaProducer[String, String](props)

  def main(args: Array[String]): Unit = {
// Set the default stream and topic to publish to.
    var topic: String = "uberrawdata"
    var fileName: String =
      "/home/prakrish/workspace/uber-large-data-analysis/src/main/resources/data/clusterslimited.txt"
    if (args.length == 2) {
      topic = args(0)
      fileName = args(1)
    }
    println("Sending to topic " + topic)
       val f: File = new File(fileName)
    val fr: FileReader = new FileReader(f)
    val reader: BufferedReader = new BufferedReader(fr)
    var line: String = reader.readLine()
    while (line != null) {
      /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */

      val rec: ProducerRecord[String, String] =
        new ProducerRecord[String, String](topic, line)
// Send the record to the producer client library.
      producer.send(rec)
      println("Sent message: " + line)
      line = reader.readLine()
      Thread.sleep(600l)
    }
    producer.close()
    println("All done.")
    System.exit(1)
  }

    
}