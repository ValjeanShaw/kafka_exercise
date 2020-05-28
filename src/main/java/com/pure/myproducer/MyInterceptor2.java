package com.pure.myproducer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 *
 * @author xiaoran
 * @date 2020/05/28
 */
public class MyInterceptor2 implements ProducerInterceptor<String, Company> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 将消息序列化和计算分区之前
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, Company> onSend(ProducerRecord<String, Company> record) {
        System.out.println("调用自定义拦截器");
        String modifiedValue = "myPrefix-" + record.value();
        Company company = new Company(record.key(),modifiedValue);
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), company, record.headers());
    }

    /**
     * 在消息被应答（Acknowledgement）之前或消息发送失败时
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率=" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
