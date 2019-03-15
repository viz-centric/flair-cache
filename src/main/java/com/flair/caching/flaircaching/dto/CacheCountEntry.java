package com.flair.caching.flaircaching.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class CacheCountEntry implements Serializable {
    private Integer count;
}
