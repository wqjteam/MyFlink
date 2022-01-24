package com.wqj.flink.producer;

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

        String data = ",zs,";
//        data = "li,zs,ww";
        String topic = "flink_dim";
//        topic = "flink_test_student";
        try {
            int i = 0;

            while (i < 100) {
                data = ((int) (Math.random() * 10000)) + ",zs,"+((int) (Math.random() * 10000));
                producer.send(new ProducerRecord<
                        String, String>(topic, data));
                System.out.println("输出第" + i + "次");
                Thread.sleep(500);
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
