package com.flair.caching.flaircaching.controllers;

import com.flair.caching.flaircaching.FlairCachingApplication;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = FlairCachingApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CacheControllerIntTest {

    @Autowired
    private WebTestClient webClient;

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("cache"));
    }

    @Test
    public void getResultNotFound() {
        this.webClient.post()
                .uri("/cache/result")
                .body(Mono.just(ImmutableMap.of("key", "somekey" + Math.random(),
                        "table", "sometable")), Map.class)
                .exchange()
                .expectStatus()
                .isNotFound()
                .expectBody()
                .isEmpty();
    }

    @Test
    public void putResult() {
        this.webClient.put()
                .uri("/cache/result")
                .body(Mono.just(ImmutableMap.of("key", "somekey",
                        "table", "sometable",
                        "value", "some value")), Map.class)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectBody()
                .isEmpty();

        this.webClient.post()
                .uri("/cache/result")
                .body(Mono.just(ImmutableMap.of("key", "somekey",
                        "table", "sometable")), Map.class)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.cache.result").isEqualTo("some value")
                .jsonPath("$.cache.dateCreated").isNotEmpty();
    }

    @Test
    public void getResultDifferentColumn() {
        String key = "somekey" + Math.random();
        this.webClient.put()
                .uri("/cache/result")
                .body(Mono.just(ImmutableMap.of(
                        "key", key,
                        "table", "sometable1",
                        "value", "some value")
                ), Map.class)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectBody()
                .isEmpty();

        this.webClient.post()
                .uri("/cache/result")
                .body(Mono.just(ImmutableMap.of(
                        "key", key,
                        "table", "sometable2")
                ), Map.class)
                .exchange()
                .expectStatus()
                .isNotFound()
                .expectBody();
    }
}