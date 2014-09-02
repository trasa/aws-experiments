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

    /*
     The exception I see, perhaps due to timeout issues (vpc vs vpn vs vpc... remote to AWS...)

     2014-09-02 11:05:44.032 ERROR net.spy.memcached.ConfigurationPoller:  Error encountered in the poller. Current cluster configuration: null
     java.lang.RuntimeException: Exception waiting for config
     at net.spy.memcached.MemcachedClient.getConfig(MemcachedClient.java:1668)
     at net.spy.memcached.ConfigurationPoller.run(ConfigurationPoller.java:115)
     at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
     at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:304)
     at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:178)
     at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
     at java.lang.Thread.run(Thread.java:745)
     Caused by: java.util.concurrent.ExecutionException: net.spy.memcached.internal.CheckedOperationTimeoutException: Operation timed out. - failing node: /10.11.110.221:11211
     at net.spy.memcached.internal.OperationFuture.get(OperationFuture.java:106)
     at net.spy.memcached.internal.GetConfigFuture.get(GetConfigFuture.java:50)
     at net.spy.memcached.MemcachedClient.getConfig(MemcachedClient.java:1656)
     ... 8 more
     Caused by: net.spy.memcached.internal.CheckedOperationTimeoutException: Operation timed out. - failing node: /10.11.110.221:11211
     ... 11 more
     2014-09-02 11:05:44.045 WARN net.spy.memcached.ConfigurationPoller:  Number of consecutive poller errors is 1. Number of minutes since the last successful polling is 0

     */

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
