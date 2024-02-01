package org.lin.canalsimpleclient.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "canal")
@Data
public class CanalServersProperties {

    private CanalProperties[] servers;

}
