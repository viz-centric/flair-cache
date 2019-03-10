package com.flair.caching.flaircaching.services;

import com.flair.caching.flaircaching.dto.CacheEntry;
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

    public Optional<CacheEntry> getResult(String table, String key) {
        log.info("Get table {} key {}", table, key);
        Optional<CacheEntry> value = cacheRepository.getResult(table, key);
        if (value.isPresent()) {
            log.info("Get value table {} key {} value {}", table, key, value.get());
        } else {
            log.info("Get value table {} key {} null", table, key);
        }
        return value;
    }

    public void putResult(String table, String key, String value,
                          Long refreshAfterDate, Long purgeAfterDate, Integer refreshAfterCount) {
        log.info("Put value table {} key {} value {} refreshAfterDate {} purgeAfterDate {} refreshAfterCount {}",
                table, key, value, refreshAfterDate, purgeAfterDate, refreshAfterCount);

        cacheRepository.putResult(table, key, value,
                refreshAfterDate,
                purgeAfterDate,
                refreshAfterCount);
    }

}
