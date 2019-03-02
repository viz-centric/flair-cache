package com.flair.caching.flaircaching.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.time.Instant;

@Data
@RequiredArgsConstructor
public class CacheMetadata {
    private final String result;
    private final Instant dateCreated;
}
