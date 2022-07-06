package com.caogen.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author 康良玉
 * @Description 描述
 * @Create 2022-07-06 10:24
 */
public class MyConsumer {

    private static KafkaConsumer<String, String> consumer;
    private static Properties properties;

    static {
        properties = new Properties();

        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "KafkaStudy7");
    }

    /**
     * 自动提交offset
     */
    private static void generalConsumeMessageAutoCommit() {
        properties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("caogen-kafka-study"));

        try {
            while (true) {
                boolean flag = true;
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format(
                            "topic = %s, partition = %s, key = %s, value = %s",
                            record.topic(), record.partition(),
                            record.key(), record.value()
                    ));
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }

                if (!flag) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 手动同步提交offset
     */
    private static void generalConsumeMessageSyncCommit() {
        properties.put("auto.commit.offset", false);
        // 从队列中的第一条消息开始消费。
        properties.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("caogen-kafka-study"));

        while (true) {
            boolean flag = true;
            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                        "topic = %s, partition = %s, offset = %s, key = %s, value = %s",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value()
                ));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            try {
                consumer.commitSync();
            } catch (CommitFailedException ex) {
                System.out.println("commit failed error: "
                        + ex.getMessage());
            }

            if (!flag) {
                break;
            }
        }
    }

    /**
     * 手动异步提交offset
     */
    private static void generalConsumeMessageAsyncCommit() {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("caogen-kafka-study"));

        while (true) {
            boolean flag = true;

            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                        "topic = %s, partition = %s, key = %s, value = %s",
                        record.topic(), record.partition(),
                        record.key(), record.value()
                ));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            // commit A, offset 2000
            // commit B, offset 3000
            consumer.commitAsync();

            if (!flag) {
                break;
            }
        }
    }

    /**
     * 手动异步提交offset,提交失败记录日志
     */
    private static void generalConsumeMessageAsyncCommitWithCallback() {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("caogen-kafka-study"));

        while (true) {
            boolean flag = true;

            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                        "topic = %s, partition = %s, key = %s, value = %s",
                        record.topic(), record.partition(),
                        record.key(), record.value()
                ));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            consumer.commitAsync((map, e) -> {
                if (e != null) {
                    System.out.println("commit failed for offsets: " +
                            e.getMessage());
                }
            });

            if (!flag) {
                break;
            }
        }
    }

    /**
     * 混合异步同步提交
     */
    private static void mixSyncAndAsyncCommit() {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("caogen-kafka-study"));

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format(
                            "topic = %s, partition = %s, key = %s, " +
                                    "value = %s",
                            record.topic(), record.partition(),
                            record.key(), record.value()
                    ));
                }

                consumer.commitAsync();
            }
        } catch (Exception ex) {
            System.out.println("commit async error: " + ex.getMessage());
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        //generalConsumeMessageAutoCommit();
        //generalConsumeMessageSyncCommit();
        generalConsumeMessageAsyncCommit();
    }

}
