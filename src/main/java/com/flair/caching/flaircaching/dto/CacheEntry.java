package com.flair.caching.flaircaching.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.Instant;

@Data
@Accessors(chain = true)
public class CacheEntry {
    private String result;
    private Instant dateCreated;
}
