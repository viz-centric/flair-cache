package com.flair.caching.flaircaching.controllers;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;

import javax.annotation.PreDestroy;

@GRpcService
@Slf4j
@RequiredArgsConstructor
public class HealthGrpcController extends HealthGrpc.HealthImplBase {

    private volatile boolean available = true;

    @Override
    public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        log.debug("Health check endpoint called");
        performHealthCheck(responseObserver);
    }

    @Override
    public void watch(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        log.debug("Streaming heath endpoint called");
        performHealthCheck(responseObserver);
    }

    @PreDestroy
    public void destroy() {
        log.info("Destroying health service");
        available = false;
    }

    private void performHealthCheck(StreamObserver<HealthCheckResponse> responseObserver) {
        responseObserver.onNext(HealthCheckResponse.newBuilder()
                .setStatus(getStatus())
                .build());
        responseObserver.onCompleted();
    }

    private HealthCheckResponse.ServingStatus getStatus() {
        if (available) {
            return HealthCheckResponse.ServingStatus.SERVING;
        }
        return HealthCheckResponse.ServingStatus.NOT_SERVING;
    }
}
