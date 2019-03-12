package com.flair.caching.flaircaching.repositories;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class CacheEntry implements Serializable {
    private String result;
    private String key;
    private Long dateCreated;
    private Long refreshAfterDate;
    private Long purgeAfterDate;
    private Integer refreshAfterCount;
}
