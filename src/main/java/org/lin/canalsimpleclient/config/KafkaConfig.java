package org.lin.canalsimpleclient.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.lin.canalsimpleclient.properties.CanalProperties;
import org.lin.canalsimpleclient.properties.CanalServersProperties;
import org.lin.canalsimpleclient.properties.KafkaTopicProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaAdmin.NewTopics newTopics(CanalServersProperties canalServersProperties) {
        List<NewTopic> topicList = new ArrayList<>();

        for (CanalProperties server : canalServersProperties.getServers()) {
            for (KafkaTopicProperties kafkaTopic : server.getKafkaTopics()) {
                if (StringUtils.isBlank(kafkaTopic.getTopic())) {
                    continue;
                }
                topicList.add(new NewTopic(kafkaTopic.getTopic(), 3, Short.parseShort("2")));
            }
        }

        KafkaAdmin.NewTopics newTopics = new KafkaAdmin.NewTopics(topicList.toArray(new NewTopic[topicList.size()]));
        return newTopics;
    }

}
