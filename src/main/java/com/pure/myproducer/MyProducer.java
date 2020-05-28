package com.pure.myproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author xiaoran
 * @date 2020/05/28
 */
public class MyProducer {
    private static final String BROKERLIST = "172.23.7.12:9092,172.23.7.10:9092,172.23.7.9:9092";
    private static final String TOPIC = "mytopic";


    public static void main(String[] args) {
        MyProducer myProducer = new MyProducer();
        myProducer.sendWithSerializer();
    }

    /**
     * 自定义分区器开始跑
     */
    public void sendWithSerializer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                MySerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);

        //发送的value值是定制化的
        KafkaProducer<String,Company> producer = new KafkaProducer<>(properties);
        ProducerRecord<String,Company> record = new ProducerRecord<>(TOPIC,new Company("alibaba","hangzhou"));

        try{
            producer.send(record);
            System.out.println("发送成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.close();
    }


}
