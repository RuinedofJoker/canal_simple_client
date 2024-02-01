package org.lin.canalsimpleclient.properties;

import lombok.Data;

@Data
public class CanalProperties {

    private String serverAddress;

    private Integer port;

    private String username;

    private String password;

    private String destination;

    private KafkaTopicProperties[] kafkaTopics;
}
