package com.pure;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author xiaoran
 * @date 2020/05/27
 */
public class ConsumerFastStart {
    private static final String brokerList = "172.23.7.12:9092";
    private static final String topic = "mytopic";
    private static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("bootstrap.servers",brokerList);
        //设置消费者组
        properties.put("group.id",groupId);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //订阅主题  collection
        consumer.subscribe(Collections.singleton(topic));
        //循环消费消息
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(5000));

            for(ConsumerRecord<String,String> record:records){
                System.out.println(record.topic()+" ---> "+record.value());
            }

        }

    }
}
