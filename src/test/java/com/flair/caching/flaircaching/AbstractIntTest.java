package com.flair.caching.flaircaching;

import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public abstract class AbstractIntTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("cache"));
    }

}
