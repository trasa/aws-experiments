package com.meancat.aws.ec;


import com.google.common.base.Stopwatch;
import net.spy.memcached.MemcachedClient;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class ObjectCacheIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ObjectCacheIntegrationTest.class);

    private static MemcachedClient client;

    private static final int RUN_COUNT = 100;

    @BeforeClass
    public static void setUpFixture() throws IOException {
        client = new MemcachedClient(new InetSocketAddress(TestEndpoints.EC_END_POINT, 11211));
    }

    @AfterClass
    public static void tearDownFixture() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void runManyTimes() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        List<Long> timing = new ArrayList<>();
        for(int i=0; i < RUN_COUNT; i++) {
            Stopwatch sw = Stopwatch.createStarted();
            run();
            timing.add(sw.elapsed(TimeUnit.MILLISECONDS));
        }

        long sum = 0;
        for (Long l : timing) {
            sum += l;
        }
        logger.info("Average Time: {}", sum / timing.size());
    }

    public void run() throws IOException, InterruptedException, ExecutionException, TimeoutException {

        // get
        Object blargh = client.get("blargh");
        logger.info("first get: {}", blargh);

        // set
        assertTrue(client.set("blargh", 5, "blargh!").get(10, TimeUnit.SECONDS));

        // get
        blargh = client.get("blargh");
        logger.info("2nd get: {}", blargh);

        // delete
        boolean delResult = client.delete("blargh").get(10, TimeUnit.SECONDS);
        logger.info("delete: {}", delResult);
    }
}
