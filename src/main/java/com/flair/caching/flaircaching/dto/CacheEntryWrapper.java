package com.flair.caching.flaircaching.dto;

import com.flair.caching.flaircaching.repositories.CacheEntry;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class CacheEntryWrapper {
    private CacheEntry cacheEntry;
    private boolean stale;
}
