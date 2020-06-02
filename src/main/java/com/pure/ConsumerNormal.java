package com.pure;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author xiaoran
 * @date 2020/06/01
 */
public class ConsumerNormal {
    private static final String BROKERLIST = "172.23.7.12:9092";
    private static final String TOPIC = "mytopic";
    private static final String GROUPID = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKERLIST);
        //设置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,GROUPID);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //订阅主题  collection
        consumer.subscribe(Collections.singleton(TOPIC));
        //循环消费消息
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(5000));

            for(ConsumerRecord<String,String> record:records){
                System.out.println(record.topic()+" ---> "+record.value());
            }

        }

    }
}
