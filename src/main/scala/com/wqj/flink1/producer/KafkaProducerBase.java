package com.wqj.flink1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaProducerBase {
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.4.110:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("request.required.acks", "-1");
         KafkaProducer producer = new KafkaProducer(kafkaProperties);
        producer = new KafkaProducer<String, String>(kafkaProperties);

        String data = "hah hah hha hh dad hhda hhad a a b b b b ";
        try {
            int i = 0;

            while (i < 500) {
                producer.send(new ProducerRecord<String, String>("flink_test",data));
                Thread.sleep(500);
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
