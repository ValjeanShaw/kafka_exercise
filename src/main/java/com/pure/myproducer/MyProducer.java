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
//        myProducer.sendWithSerializer();
//        myProducer.sendWithPartition();
//        myProducer.sendWithMyPartition();
        myProducer.sendWithInterceptor();

        //拦截器-分区器-序列化器
        myProducer.sendWithLink();
    }

    /**
     * 自定义序列化方式开始跑
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


    public void sendWithPartition(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);


        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        //无key
        ProducerRecord<String,String> record0 = new ProducerRecord<>(TOPIC,"beijing");
        //指定key
        ProducerRecord<String,String> record1 = new ProducerRecord<>(TOPIC,"alibaba","hangzhou");
        //指定分区
        ProducerRecord<String,String> record2 = new ProducerRecord<>(TOPIC,2,"tencent","shenzhen");

        try{
            producer.send(record0);
            producer.send(record1);
            producer.send(record2);

            System.out.println("发送成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.close();
    }

    /**
     * 自定义分区器
     */
    public void sendWithMyPartition(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class.getName());


        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> record0 = new ProducerRecord<>(TOPIC,"chengdu");

        for (int i = 0; i < 10; i++) {
            try{
                producer.send(record0);
                System.out.println("发送成功");
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        producer.close();
    }

    /**
     * 自定义拦截器
     */
    public void sendWithInterceptor(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        //可以集成多个，按照逗号隔开，先执行前者
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyInterceptor.class.getName()+","+MyInterceptor.class.getName());


        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> record0 = new ProducerRecord<>(TOPIC,"chengdu");

        try{
            producer.send(record0);
            System.out.println("发送拦截器之后的消息成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.close();
    }


    public void sendWithLink(){
        Properties properties = new Properties();

        //自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class.getName());

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //自定义序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MySerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);

        //可以集成多个，按照逗号隔开，先执行前者
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyInterceptor2.class.getName());

        //发送的value值是定制化的
        KafkaProducer<String,Company> producer = new KafkaProducer<>(properties);
        ProducerRecord<String,Company> record = new ProducerRecord<>(TOPIC,new Company("alibaba","hangzhou"));

        try{
            producer.send(record);
            System.out.println("发送处理后的消息成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.close();
    }

}
