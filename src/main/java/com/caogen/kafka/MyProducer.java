package com.caogen.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Author 康良玉
 * @Description 描述
 * @Create 2022-07-05 17:36
 */
public class MyProducer {

    private static KafkaProducer<String, String> producer;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("partitioner.class",
                "com.caogen.kafka.CustomPartitioner");

        producer = new KafkaProducer<>(properties);
    }

    /**
     * 不管发送是否成功的发送消息
     */
    private static void sendMessageForgetResult() {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "caogen-kafka-study", "name", "ForgetResult"
        );
        producer.send(record);
        producer.close();
    }

    /**
     * 同步发送消息
     * @throws Exception
     */
    private static void sendMessageSync() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "caogen-kafka-study", "name", "sync"
        );
        RecordMetadata result = producer.send(record).get();

        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());

        producer.close();
    }

    /**
     * 异步发送消息
     */
    private static void sendMessageCallback() {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "caogen-kafka-study", "name", "callback"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "caogen-kafka-study", "name-x", "callback"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "caogen-kafka-study", "name-y", "callback"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "caogen-kafka-study", "name-z", "callback"
        );
        producer.send(record, new MyProducerCallback());

        producer.close();
    }

    private static class MyProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
                return;
            }

            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }

    public static void main(String[] args) throws Exception {
        //sendMessageForgetResult();
        //sendMessageSync();
        sendMessageCallback();
    }

}
