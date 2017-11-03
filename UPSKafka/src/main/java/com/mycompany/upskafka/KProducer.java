package com.mycompany.upskafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KProducer {

    public void produce() {
        Logger logger = LoggerFactory.getLogger("my_producer");
        
        logger.info("Starting producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        
        props.put("schema.registry.url", "http://0.0.0.0:8081");
        KafkaProducer producer = new KafkaProducer(props);
        
        String key = "key1";
        String userSchema = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value1");
        logger.info("avroRecord: " + avroRecord.toString());
//        ProducerRecord<Object, Object> record = new ProducerRecord<>("ups", key, avroRecord);
        ProducerRecord<Object, Object> record = new ProducerRecord<>("ups", null, avroRecord);
        try {
            for (int i=0; i <10; i++){
                producer.send(record);
                logger.info("Sending " + record.toString());
            }
            producer.flush();
            
        } catch (Exception e) {
            // may need to do something with it
            logger.error(e.toString());
        }
        logger.info("Exit producer...");
    }
    

}
