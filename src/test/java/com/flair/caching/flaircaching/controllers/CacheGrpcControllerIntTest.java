package com.flair.caching.flaircaching.controllers;

import com.flair.bi.messages.CacheServiceGrpc;
import com.flair.bi.messages.GetCacheRequest;
import com.flair.bi.messages.GetCacheResponse;
import com.flair.bi.messages.PutCacheRequest;
import com.flair.caching.flaircaching.services.CacheService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class CacheGrpcControllerIntTest {

    private static final long CURRENT_TIMESTAMP = 1552241431;

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Autowired
    private CacheService cacheService;

    private CacheServiceGrpc.CacheServiceBlockingStub blockingStub;

    @TestConfiguration
    static class TestClockConfig {
        @Bean
        @Primary
        public Clock testClock() {
            return Clock.fixed(Instant.ofEpochSecond(CURRENT_TIMESTAMP), ZoneId.of("Z"));
        }
    }

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("cache"));

        String serverName = InProcessServerBuilder.generateName();

        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(new CacheGrpcController(cacheService))
                .build()
                .start());

        blockingStub = CacheServiceGrpc.newBlockingStub(
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @Test
    public void getCacheNotFound() {
        boolean exceptionHandled = false;
        try {
            blockingStub.getCache(GetCacheRequest.newBuilder()
                    .setTable("table")
                    .setKey("key" + Math.random() + System.currentTimeMillis())
                    .build());
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                exceptionHandled = true;
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
                assertEquals(Status.NOT_FOUND.getCode(), statusRuntimeException.getStatus().getCode());
                assertEquals("errors.cache.not_found", statusRuntimeException.getStatus().getDescription());
            }
        }

        assertTrue(exceptionHandled);
    }

    @Test
    public void getResultInvalidRequestKey() {
        boolean exceptionHandled = false;
        try {
            blockingStub.getCache(GetCacheRequest.newBuilder()
                    .setTable("table")
                    .setKey("")
                    .build());
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                exceptionHandled = true;
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
                assertEquals(Status.INVALID_ARGUMENT.getCode(), statusRuntimeException.getStatus().getCode());
                assertEquals("errors.cache.key.null", statusRuntimeException.getStatus().getDescription());
            }
        }

        assertTrue(exceptionHandled);
    }

    @Test
    public void putResultInvalidRequestKey() {
        boolean exceptionHandled = false;
        try {
            blockingStub.putCache(PutCacheRequest.newBuilder()
                    .setTable("table")
                    .setKey("")
                    .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                    .setRefreshAfterCount(1)
                    .setRefreshAfterDate(CURRENT_TIMESTAMP + 10)
                    .build());
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                exceptionHandled = true;
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
                assertEquals(Status.INVALID_ARGUMENT.getCode(), statusRuntimeException.getStatus().getCode());
                assertEquals("errors.cache.key.null", statusRuntimeException.getStatus().getDescription());
            }
        }

        assertTrue(exceptionHandled);
    }

    @Test
    public void putResultInvalidRequestTable() {
        boolean exceptionHandled = false;
        try {
            blockingStub.putCache(PutCacheRequest.newBuilder()
                    .setTable("")
                    .setKey("key" + Math.random() + System.currentTimeMillis())
                    .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                    .setRefreshAfterCount(1)
                    .setRefreshAfterDate(CURRENT_TIMESTAMP + 10)
                    .build());
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                exceptionHandled = true;
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
                assertEquals(Status.INVALID_ARGUMENT.getCode(), statusRuntimeException.getStatus().getCode());
                assertEquals("errors.cache.table.null", statusRuntimeException.getStatus().getDescription());
            }
        }

        assertTrue(exceptionHandled);
    }

    @Test
    public void getResultInvalidRequestTable() {
        boolean exceptionHandled = false;
        try {
            blockingStub.getCache(GetCacheRequest.newBuilder()
                    .setTable("")
                    .setKey("key" + Math.random() + System.currentTimeMillis())
                    .build());
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                exceptionHandled = true;
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
                assertEquals(Status.INVALID_ARGUMENT.getCode(), statusRuntimeException.getStatus().getCode());
                assertEquals("errors.cache.table.null", statusRuntimeException.getStatus().getDescription());
            }
        }

        assertTrue(exceptionHandled);
    }

    @Test
    public void getResult() {
        String key = "key" + Math.random() + System.currentTimeMillis();

        blockingStub.putCache(PutCacheRequest.newBuilder()
                .setTable("table")
                .setValue("value")
                .setKey(key)
                .setRefreshAfterDate(CURRENT_TIMESTAMP + 10)
                .setRefreshAfterCount(1)
                .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                .build());

        GetCacheResponse cache = blockingStub.getCache(GetCacheRequest.newBuilder()
                .setTable("table")
                .setKey(key)
                .build());

        assertEquals("value", cache.getResult());
        assertEquals(CURRENT_TIMESTAMP, cache.getMetadata().getDateCreated());
        assertFalse(cache.getMetadata().getStale());
    }

    @Test
    public void getResultStaleBecauseOfDate() {
        String key = "key" + Math.random() + System.currentTimeMillis();

        blockingStub.putCache(PutCacheRequest.newBuilder()
                .setTable("table")
                .setValue("value")
                .setKey(key)
                .setRefreshAfterDate(CURRENT_TIMESTAMP - 1)
                .setRefreshAfterCount(1)
                .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                .build());

        GetCacheResponse cache = blockingStub.getCache(GetCacheRequest.newBuilder()
                .setTable("table")
                .setKey(key)
                .build());

        assertEquals("value", cache.getResult());
        assertEquals(CURRENT_TIMESTAMP, cache.getMetadata().getDateCreated());
        assertTrue(cache.getMetadata().getStale());
    }

    @Test
    public void getResultNotStaleBecauseOfDateIfNoDateProvided() {
        String key = "key" + Math.random() + System.currentTimeMillis();

        blockingStub.putCache(PutCacheRequest.newBuilder()
                .setTable("table")
                .setValue("value")
                .setKey(key)
                .setRefreshAfterCount(2)
                .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                .build());

        GetCacheResponse cache = blockingStub.getCache(GetCacheRequest.newBuilder()
                .setTable("table")
                .setKey(key)
                .build());

        assertEquals("value", cache.getResult());
        assertEquals(CURRENT_TIMESTAMP, cache.getMetadata().getDateCreated());
        assertFalse(cache.getMetadata().getStale());
    }

    @Test
    public void getResultStaleBecauseOfCount() {
        String key = "key" + Math.random() + System.currentTimeMillis();

        PutCacheRequest putRequest = PutCacheRequest.newBuilder()
                .setTable("table")
                .setValue("value")
                .setKey(key)
                .setRefreshAfterDate(CURRENT_TIMESTAMP + 60)
                .setRefreshAfterCount(2)
                .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                .build();
        blockingStub.putCache(putRequest);

        GetCacheRequest getRequest = GetCacheRequest.newBuilder()
                .setTable("table")
                .setKey(key)
                .build();
        blockingStub.getCache(getRequest);
        blockingStub.getCache(getRequest);
        GetCacheResponse cache = blockingStub.getCache(getRequest);

        assertEquals("value", cache.getResult());
        assertEquals(CURRENT_TIMESTAMP, cache.getMetadata().getDateCreated());
        assertTrue(cache.getMetadata().getStale());
    }

    @Test
    public void getResultDoesNotReturnStaleBecauseOfCountIfWriteWasInBetween() {
        String key = "key" + Math.random() + System.currentTimeMillis();

        PutCacheRequest putRequest = PutCacheRequest.newBuilder()
                .setTable("table")
                .setValue("value")
                .setKey(key)
                .setRefreshAfterDate(CURRENT_TIMESTAMP + 60)
                .setRefreshAfterCount(2)
                .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                .build();
        blockingStub.putCache(putRequest);

        GetCacheRequest getRequest = GetCacheRequest.newBuilder()
                .setTable("table")
                .setKey(key)
                .build();
        blockingStub.getCache(getRequest);
        blockingStub.putCache(putRequest);
        blockingStub.getCache(getRequest);
        GetCacheResponse cache = blockingStub.getCache(getRequest);

        assertEquals("value", cache.getResult());
        assertEquals(CURRENT_TIMESTAMP, cache.getMetadata().getDateCreated());
        assertFalse(cache.getMetadata().getStale());
    }

    @Test
    public void getResultDoesNotReturnStaleBecauseOfCountIfNoCountProvided() {
        String key = "key" + Math.random() + System.currentTimeMillis();

        PutCacheRequest putRequest = PutCacheRequest.newBuilder()
                .setTable("table")
                .setValue("value")
                .setKey(key)
                .setRefreshAfterDate(CURRENT_TIMESTAMP + 60)
                .setPurgeAfterDate(CURRENT_TIMESTAMP + 60)
                .build();
        blockingStub.putCache(putRequest);

        GetCacheRequest getRequest = GetCacheRequest.newBuilder()
                .setTable("table")
                .setKey(key)
                .build();
        blockingStub.getCache(getRequest);
        blockingStub.getCache(getRequest);
        GetCacheResponse cache = blockingStub.getCache(getRequest);

        assertEquals("value", cache.getResult());
        assertEquals(CURRENT_TIMESTAMP, cache.getMetadata().getDateCreated());
        assertFalse(cache.getMetadata().getStale());
    }

    @Test
    public void getResultDifferentTable() {
        String key = "key" + Math.random() + System.currentTimeMillis();
        String key2 = "key" + Math.random() + System.currentTimeMillis();

        blockingStub.putCache(PutCacheRequest.newBuilder()
                .setTable("table1")
                .setValue("value1")
                .setKey(key)
                .build());

        blockingStub.putCache(PutCacheRequest.newBuilder()
                .setTable("table1")
                .setValue("value_1")
                .setKey(key2)
                .build());

        blockingStub.putCache(PutCacheRequest.newBuilder()
                .setTable("table2")
                .setValue("value2")
                .setKey(key)
                .build());

        GetCacheResponse cache1 = blockingStub.getCache(GetCacheRequest.newBuilder()
                .setTable("table1")
                .setKey(key)
                .build());

        GetCacheResponse cache2 = blockingStub.getCache(GetCacheRequest.newBuilder()
                .setTable("table2")
                .setKey(key)
                .build());

        GetCacheResponse cache3 = blockingStub.getCache(GetCacheRequest.newBuilder()
                .setTable("table1")
                .setKey(key2)
                .build());

        assertEquals("value1", cache1.getResult());
        assertEquals("value2", cache2.getResult());
        assertEquals("value_1", cache3.getResult());
    }

}
