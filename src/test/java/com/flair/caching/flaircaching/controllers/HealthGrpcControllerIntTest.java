package com.flair.caching.flaircaching.controllers;

import com.flair.caching.flaircaching.AbstractIntTest;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HealthGrpcControllerIntTest extends AbstractIntTest {

    private HealthGrpc.HealthBlockingStub blockingStub;
    private HealthGrpcController controller;

    @Override
    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        controller = new HealthGrpcController();
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(controller)
                .build()
                .start());

        blockingStub = HealthGrpc.newBlockingStub(
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @Test
    public void check() {
        HealthCheckResponse response = blockingStub.check(HealthCheckRequest.newBuilder()
                .setService("my-service")
                .build());
        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    }

    @Test
    public void watch() {
        HealthCheckResponse response = blockingStub.check(HealthCheckRequest.newBuilder()
                .setService("my-service")
                .build());
        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    }

    @Test
    public void checkAfterDestroy() {
        controller.destroy();
        HealthCheckResponse response = blockingStub.check(HealthCheckRequest.newBuilder()
                .setService("my-service")
                .build());
        assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, response.getStatus());
    }
}