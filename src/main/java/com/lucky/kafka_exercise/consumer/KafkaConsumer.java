package com.lucky.kafka_exercise.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author: xiaoran
 * @date: 2019-03-23 15:33
 * <p>
 * 1.同一个consumer-group  同一个topic下的消息只被消费一次,即只能有一个consumer消费消息   不同consumer-group下，消息都会被多个consumer消费
 * 2.同一topic   processMessage(String content)和processMessage(ConsumerRecord<?, ?> record)  优先选择匹配 string content
 */
@Component
@Slf4j
public class KafkaConsumer {
    @KafkaListener(groupId = "group1", topics = "my-test")
    public void processMessage(ConsumerRecord<?, ?> record) {
        log.info("group1  my-test消费信息：{}", record.toString());
    }

    @KafkaListener(groupId = "group1", topics = {"my-test2"})
    public void processMessage(String content) {
        log.info("group1 my-test2消费消息：{}", content);
    }

    @KafkaListener(groupId = "group2", topics = "my-test")
    public void processMessage2(String content) {
        log.info("group2 my-test消费消息：{}", content);
    }

    @KafkaListener(groupId = "group2", topics = "my-test")
    public void processMessage2(ConsumerRecord<?, ?> record) {
        log.info("group2 接收到的 my-test 所有信息 {}", record.toString());
    }


    @KafkaListener(groupId = "group2", topics = {"my-test", "my-test2"})
    public void processMessageMutilTopic(String content) {
        log.info("group2 多个topic消费消息：{}", content);
    }


}
