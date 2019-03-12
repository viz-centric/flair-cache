package com.flair.caching.flaircaching.services;

import com.flair.caching.flaircaching.dto.CacheCountEntry;
import com.flair.caching.flaircaching.dto.CacheEntryWrapper;
import com.flair.caching.flaircaching.repositories.CacheEntry;
import com.flair.caching.flaircaching.repositories.CacheEntryResult;
import com.flair.caching.flaircaching.repositories.CacheRepository;
import com.flair.caching.flaircaching.utils.BooleanCondition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    private final CacheRepository cacheRepository;
    private final Clock clock;

    public Optional<CacheEntryWrapper> getResult(String table, String key) {
        log.info("Get table {} key {}", table, key);
        CacheEntryResult cacheEntryResult = cacheRepository.getResult(table, key);

        if (cacheEntryResult.getCacheEntry() == null) {
            log.info("Get value table {} key {} null", table, key);
            return Optional.empty();
        }

        log.info("Get value table {} key {} value {}", table, key, cacheEntryResult);

        CacheEntry cacheEntry = cacheEntryResult.getCacheEntry();
        CacheCountEntry cacheCountEntry = cacheEntryResult.getCacheCountEntry();

        CacheEntryWrapper cacheEntryWrapper = new CacheEntryWrapper()
                .setCacheEntry(cacheEntry);

        List<BooleanCondition> cacheStalenessChecksList = Arrays.asList(
                () -> checkStaleRefreshDate(cacheEntry),
                () -> checkStaleReadCount(cacheEntry, cacheCountEntry, key, table)
        );

        for (BooleanCondition check : cacheStalenessChecksList) {
            if (check.test()) {
                cacheEntryWrapper.setStale(true);
                break;
            }
        }

        return Optional.of(cacheEntryWrapper);
    }

    private boolean checkStaleReadCount(CacheEntry cacheEntry, CacheCountEntry cacheCountEntry, String key, String table) {
        if (cacheEntry.getRefreshAfterCount() == 0) {
            return false;
        }

        cacheCountEntry = Optional.ofNullable(cacheCountEntry)
                .orElseGet(() -> new CacheCountEntry().setCount(0));

        boolean stale = cacheEntry.getRefreshAfterCount() <= cacheCountEntry.getCount();
        if (!stale) {
            cacheCountEntry.setCount(cacheCountEntry.getCount() + 1);
            cacheRepository.putCount(key, table, cacheCountEntry);
        }
        return stale;
    }

    private boolean checkStaleRefreshDate(CacheEntry cacheEntry) {
        if (cacheEntry.getRefreshAfterDate() == 0) {
            return false;
        }
        return cacheEntry.getRefreshAfterDate() < Instant.now(clock).getEpochSecond();
    }

    public void putResult(String table, String key, String value,
                          Long refreshAfterDate, Long purgeAfterDate, Integer refreshAfterCount) {
        log.info("Put value table {} key {} value {} refreshAfterDate {} purgeAfterDate {} refreshAfterCount {}",
                table, key, value, refreshAfterDate, purgeAfterDate, refreshAfterCount);

        long epochSecond = Instant.now(clock).getEpochSecond();


        CacheCountEntry cacheCountEntry = null;
        if (refreshAfterCount != 0) {
            cacheCountEntry = cacheRepository.getCount(key, table)
                    .orElseGet(() -> new CacheCountEntry().setCount(0));
            cacheCountEntry.setCount(0);
        }

        cacheRepository.putResult(table, key, value,
                refreshAfterDate,
                purgeAfterDate,
                refreshAfterCount,
                epochSecond,
                cacheCountEntry);
    }

}
