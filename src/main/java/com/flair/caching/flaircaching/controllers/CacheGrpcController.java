package com.flair.caching.flaircaching.controllers;

import com.flair.bi.messages.CacheMetadata;
import com.flair.bi.messages.CacheServiceGrpc;
import com.flair.bi.messages.GetCacheRequest;
import com.flair.bi.messages.GetCacheResponse;
import com.flair.bi.messages.PutCacheRequest;
import com.flair.bi.messages.PutCacheResponse;
import com.flair.caching.flaircaching.services.CacheService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.lognet.springboot.grpc.GRpcService;

import java.time.Instant;
import java.util.Optional;

@GRpcService
@Slf4j
@RequiredArgsConstructor
public class CacheGrpcController extends CacheServiceGrpc.CacheServiceImplBase {

    private final CacheService cacheService;

    @Override
    public void getCache(GetCacheRequest request, StreamObserver<GetCacheResponse> responseObserver) {
        if (StringUtils.isEmpty(request.getKey())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("errors.cache.key.null")
                    .asRuntimeException());
            return;
        }

        if (StringUtils.isEmpty(request.getTable())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("errors.cache.table.null")
                    .asRuntimeException());
            return;
        }

        Optional<String> cacheResult = cacheService.getResult(request.getTable(), request.getKey());

        cacheResult.ifPresent(rawCache -> {
            responseObserver.onNext(GetCacheResponse.newBuilder()
                    .setResult(rawCache)
                    .setMetadata(CacheMetadata.newBuilder()
                            .setDateCreated(Instant.now().toEpochMilli())
                            .build())
                    .build());
            responseObserver.onCompleted();
        });

        if (!cacheResult.isPresent()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("errors.cache.not_found")
                    .asRuntimeException());
        }
    }

    @Override
    public void putCache(PutCacheRequest request, StreamObserver<PutCacheResponse> responseObserver) {
        if (StringUtils.isEmpty(request.getKey())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("errors.cache.key.null")
                    .asRuntimeException());
            return;
        }

        if (StringUtils.isEmpty(request.getTable())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("errors.cache.table.null")
                    .asRuntimeException());
            return;
        }

        cacheService.putResult(request.getTable(), request.getKey(), request.getValue());

        responseObserver.onNext(PutCacheResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
