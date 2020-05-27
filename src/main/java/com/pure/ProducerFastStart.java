package com.pure;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author xiaoran
 * @date 2020/05/27
 */
public class ProducerFastStart {

    private static final String BROKERLIST = "172.23.7.12:9092";
    private static final String TOPIC = "mytopic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        properties.put("bootstrap.servers",BROKERLIST);

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,"你好");

        try{
            producer.send(record);
            System.out.println("发送成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.close();
    }


}
