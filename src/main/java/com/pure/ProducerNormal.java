package com.pure;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author xiaoran
 * @date 2020/05/28
 */
public class ProducerNormal {
    private static final String BROKERLIST = "172.23.7.12:9092";
    private static final String TOPIC = "mytopic";

    public static void main(String[] args) {
        //防止写字符串方式出错
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "normal message");

        try {
            producer.send(record);
            System.out.println("发送成功");
        } catch (Exception e) {
            e.printStackTrace();
        }


        record = new ProducerRecord<>(TOPIC, "normal message callback");

        //带有callback 回调接口方式的send
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition() + ":" + recordMetadata.offset());
                }
            }
        });


        record = new ProducerRecord<>(TOPIC, "normal message callback lamda");

        //带有callback 回调接口方式的send lamda
        producer.send(record, (recordMetadata,e) ->{
            if (e != null) {
                e.printStackTrace();
            } else {
                System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition() + ":" + recordMetadata.offset());
            }
        });

        producer.close();
    }
}
