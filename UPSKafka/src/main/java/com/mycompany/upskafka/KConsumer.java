/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.upskafka;

import org.apache.avro.generic.IndexedRecord;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
//import kafka.message.MessageAndMetadata;
//import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KConsumer {

    public static void consume() {
        Logger logger = LoggerFactory.getLogger("my_consumer");
        List<String> brokers = new ArrayList<>();
        brokers.add("localhost:9092");
        logger.info("Starting consumer...");
        Properties props = new Properties();
        //props.put("zookeeper.connect", "129.146.75.92:2181");
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "ben2");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("key.deserializer",
            io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("value.deserializer",
            io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("session.timeout.ms", 30000);
        props.put("auto.offset.reset", "earliest");
        String topic = "ups";
//        Map<String, Integer> topicCountMap = new HashMap<>();
//
//        topicCountMap.put(topic, new Integer(1));
//
//        VerifiableProperties vProps = new VerifiableProperties(props);
//        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
//        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
//
//        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        logger.info("tag 1");
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);
        logger.info("tag 2");
        //consumer.subscribe(Arrays.asList(topic));
        consumer.assign(Arrays.asList(new TopicPartition(topic,0)));
        logger.info("tag 3");
        consumer.poll(10000).forEach(record -> {
            logger.info("tag 4");
            logger.info(record.toString());
        });
        logger.info("tag 5");
       

//        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
//                topicCountMap, keyDecoder, valueDecoder);
//        
//        KafkaStream stream = consumerMap.get(topic).get(0);
//        ConsumerIterator it = stream.iterator();
//
//        System.out.println("send");
//        System.out.println("I'm here");
//        while (it.hasNext()) {
//            System.out.println("in while");
//            MessageAndMetadata messageAndMetadata = it.next();
//            System.out.println("tag 1");
//            try {
//                String key = (String) messageAndMetadata.key();
//                IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
//                System.out.println(key);
//                System.out.println(value.toString());
//
//            } catch (SerializationException e) {
//                System.out.println(e.getCause());
//                System.out.println(e.getMessage());
//                // may need to do something with it
//            }
//        }
//        System.out.println("At end");
    }
}
