package com.flair.caching.flaircaching.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "grpc", ignoreUnknownFields = false)
@Component
@Data
public class GrpcProperties {

    private Tls tls = new Tls();
    private Long port;
    private boolean enabled;

    @Data
    public static class Tls {
        private boolean enabled;
        private String certChainFile;
        private String privateKeyFile;
        private String trustCertCollectionFile;
    }

}
