package com.flair.caching.flaircaching.services;

import com.flair.caching.flaircaching.repositories.CacheRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    private final CacheRepository cacheRepository;

    public Optional<String> getResult(String table, String key) {
        log.info("Get table {} key {}", table, key);
        Optional<String> value = cacheRepository.getResult(table, key);
        if (value.isPresent()) {
            log.info("Get value table {} key {} value {}", table, key, value.get());
        } else {
            log.info("Get value table {} key {} null", table, key);
        }
        return value;
    }

    public void putResult(String table, String key, String value) {
        log.info("Put value table {} key {} value {}", table, key, value);
        cacheRepository.putResult(table, key, value);
    }

}
