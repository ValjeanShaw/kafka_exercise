package com.lucky.kafka_exercise.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author: xiaoran
 * @date: 2019-03-23 15:28
 */
@Controller
@RequestMapping("/kafka")
public class KafkaController {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    private final static String TOPIC_1 = "my-test";
    private final static String TOPIC_2 = "my-test2";

    /**
     * 发送一个消息  一个topic
     * @param param
     * @return
     */
    @RequestMapping(value = "/send", method = RequestMethod.POST,produces = {"application/json"})
    @ResponseBody
    public String sendKafka(@RequestBody String param) {
        kafkaTemplate.send(TOPIC_1, param);
        return param;
    }

    /**
     * 发送一个消息  多个topic
     * @param param
     * @return
     */
    @RequestMapping(value = "/send2", method = RequestMethod.POST,produces = {"application/json"})
    @ResponseBody
    public String sendKafkaMutilTopic(@RequestBody String param) {
        kafkaTemplate.send(TOPIC_1, param);
        kafkaTemplate.send(TOPIC_2, param+"---2");
        return param;
    }
}
