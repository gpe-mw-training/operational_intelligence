<<<<<<< HEAD
/**
 * 
 */
=======

>>>>>>> 5b3da222b0c6d92e74892317a63be9fb513f3474
package com.streamskafka.uber;

/**
 * @author prakrish
 *
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class MsgProducer {
	 // Declare a new producer
    public static KafkaProducer producer;

    public static void main(String[] args) throws Exception {

        // Set the default stream and topic to publish to.
        String topic = "UberTopic";
<<<<<<< HEAD
        String fileName = "/home/prakrish/workspace/uber-large-data-analysis/src/main/resources/data/cluster.txt";
=======
        String fileName = "/opt/zeppelin/notebook/data/cluster.txt";
>>>>>>> 5b3da222b0c6d92e74892317a63be9fb513f3474

        if (args.length == 2) {
            topic = args[0];
            fileName = args[1];

        }
        System.out.println("Sending to topic " + topic);
        configureProducer();
        File f = new File(fileName);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {
  
            /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic,  line);

            // Send the record to the producer client library.
            producer.send(rec);
            System.out.println("Sent message: " + line);
            line = reader.readLine();
             Thread.sleep(600l);

        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);

    }

    /* Set the value for a configuration parameter.
     This configuration parameter specifies which class
     to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

}

