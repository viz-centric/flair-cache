package com.flair.caching.flaircaching;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class FlairCachingApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlairCachingApplication.class, args);
    }

}
