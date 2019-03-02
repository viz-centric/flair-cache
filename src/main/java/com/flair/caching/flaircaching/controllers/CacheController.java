package com.flair.caching.flaircaching.controllers;

import com.flair.caching.flaircaching.dto.CacheMetadata;
import com.flair.caching.flaircaching.services.CacheService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Optional;

@RestController
@Slf4j
@RequestMapping("/cache")
@RequiredArgsConstructor
public class CacheController {

    private final CacheService cacheService;

    @PostMapping("/result")
    public ResponseEntity<CacheResultResponse> getResult(@Valid @RequestBody CacheController.GetCacheRequest request) {
        Optional<String> result = cacheService.getResult(request.getTable(), request.getKey());
        return result
                .map(it -> ResponseEntity.ok(new CacheResultResponse(new CacheMetadata(it, Instant.now()))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/result")
    public ResponseEntity<CacheResultResponse> putResult(@Valid @RequestBody CacheController.PutCacheRequest request) {
        cacheService.putResult(request.getTable(), request.getKey(), request.getValue());
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @Data
    private static class GetCacheRequest {
        @NotEmpty(message = "error.cache.get.key.null")
        String key;
        @NotEmpty(message = "error.cache.get.table.null")
        String table;
    }

    @Data
    private static class PutCacheRequest {
        @NotEmpty(message = "error.cache.put.key.null")
        String key;
        @NotNull(message = "error.cache.put.value.null")
        String value;
        @NotEmpty(message = "error.cache.put.table.null")
        String table;
    }

    @Data
    @RequiredArgsConstructor
    private static class CacheResultResponse {
        final CacheMetadata cache;
    }
}
