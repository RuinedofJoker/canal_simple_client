package org.lin.canalsimpleclient.properties;

import lombok.Data;

@Data
public class KafkaTopicProperties {

    private String topic;

    private Integer partition;

    private String subscribe;
}
