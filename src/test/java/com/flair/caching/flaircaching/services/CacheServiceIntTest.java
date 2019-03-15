package com.flair.caching.flaircaching.services;

import com.flair.caching.flaircaching.AbstractIntTest;
import com.flair.caching.flaircaching.dto.CacheEntryWrapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CacheServiceIntTest extends AbstractIntTest {

    @Autowired
    private CacheService cacheService;

    @Test
    public void purgeOnlyCacheEntry() {
        String key = "key" + Math.random() + System.currentTimeMillis();
        cacheService.putResult("table", key,
                "value1", 0L, Instant.now().getEpochSecond() - 1, 0);

        Optional<CacheEntryWrapper> result = cacheService.getResult("table", key);
        assertEquals("value1", result.get().getCacheEntry().getResult());

        cacheService.purge();

        result = cacheService.getResult("table", key);
        assertFalse(result.isPresent());
    }
}
