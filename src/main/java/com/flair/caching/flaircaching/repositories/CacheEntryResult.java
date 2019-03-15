package com.flair.caching.flaircaching.repositories;

import com.flair.caching.flaircaching.dto.CacheCountEntry;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class CacheEntryResult {
    private CacheEntry cacheEntry;
    private CacheCountEntry cacheCountEntry;
}
