package org.lin.canalsimpleclient.listener;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TableStateListener {

    @KafkaListener(id = "test1listener1", topics = "test1", groupId = "test1Group")
    public void test1listener1(ConsumerRecord<String, String> msg, Acknowledgment ack) {
        Long offset = null;
        try {
            offset = msg.offset();

            String key = msg.key();
            JSONObject value = JSON.parseObject(msg.value());

            System.out.println(key);
            System.out.println(value);

            ack.acknowledge();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (offset != null) {
                ack.nack(10000);
            }
        }
    }
}
