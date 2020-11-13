package com.wanfadger.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        //steps involved
        /*
         create kafka properties
         create kafka records
         create a producer
         send/publish data to a topic
         */
         final String bootstrapServer = "localhost:9092";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class);

        KafkaProducer<String , String> kafkaProducer = new KafkaProducer<>(properties);

        //record takes three parameters
        /*
         topic optional (1st parameter)
         key optional (2nd parameter)
         message compulsory (3rd parameter)
         */
        ProducerRecord<String , String> record = new ProducerRecord<>("myfirst" , "publishing from java");

        kafkaProducer.send(record);
        kafkaProducer.flush();
        kafkaProducer.close();

        System.out.println( "Hello World!" );
    }
}
