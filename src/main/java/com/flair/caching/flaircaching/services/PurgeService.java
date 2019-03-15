package com.flair.caching.flaircaching.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PurgeService {

    private final CacheService cacheService;

    @Scheduled(fixedDelay = 3_600_000)
    public void schedulePurge() {
        cacheService.purge();
    }
}
