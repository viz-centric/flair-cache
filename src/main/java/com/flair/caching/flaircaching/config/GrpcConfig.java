package com.flair.caching.flaircaching.config;

import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class GrpcConfig extends GRpcServerBuilderConfigurer {

    @Autowired
    private GrpcProperties grpcProperties;

    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        log.info("Grpc config: Configuring grpc {}", grpcProperties.getTls());
        if (grpcProperties.getTls().isEnabled()) {
            NettyServerBuilder nsb = (NettyServerBuilder) serverBuilder;
            try {
                nsb.sslContext(getSslContextBuilder().build());
            } catch (Throwable e) {
                log.error("Grpc config: Error configuring ssl", e);
            }
        }
    }

    private SslContextBuilder getSslContextBuilder() {
        log.info("Grpc config: Configuring ssl cert {} key {} trust {}",
                grpcProperties.getTls().getCertChainFile(), grpcProperties.getTls().getPrivateKeyFile(), grpcProperties.getTls().getTrustCertCollectionFile());

        SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(
                new File(grpcProperties.getTls().getCertChainFile()),
                new File(grpcProperties.getTls().getPrivateKeyFile())
        );

        if (grpcProperties.getTls().getTrustCertCollectionFile() != null) {
            sslClientContextBuilder.trustManager(new File(grpcProperties.getTls().getTrustCertCollectionFile()));
            sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
        }
        return GrpcSslContexts.configure(sslClientContextBuilder, SslProvider.OPENSSL);
    }
}
