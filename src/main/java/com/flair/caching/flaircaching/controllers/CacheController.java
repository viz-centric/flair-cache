package com.flair.caching.flaircaching.controllers;

import com.flair.caching.flaircaching.services.CacheService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController("/cache")
@Slf4j
@RequiredArgsConstructor
public class CacheController {

    private final CacheService cacheService;

    @PostMapping("/result")
    public ResponseEntity<String> getResult(@Valid @RequestBody CacheResultRequest request) {
        return ResponseEntity.ok().build();
    }

    @Data
    private static class CacheResultRequest {
        String result;
        String connectionLinkId;
    }

}
