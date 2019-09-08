package com.flair.caching.flaircaching.repositories;

public class CacheRuntimeException extends RuntimeException {
    public CacheRuntimeException(String message, Exception exception) {
        super(message, exception);
    }
}
