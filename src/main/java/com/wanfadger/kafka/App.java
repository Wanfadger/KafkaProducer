package com.wanfadger.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        final ProducerRecord<String , String> record = new ProducerRecord<>("myfirst" , "publishing from java b y wanfadger");

        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                Logger logger = LoggerFactory.getLogger(App.class);
                if (e==null) {
                    logger.info(
                            "topic "+recordMetadata.topic()
                                    +" partition "+recordMetadata.partition()
                            +" offset "+recordMetadata.offset()
                            +" timestamp "+recordMetadata.timestamp()
                    );
                }else{
                    logger.error("cannot produce , getting error "+e);
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();

        System.out.println( "Hello World!" );
    }
}
