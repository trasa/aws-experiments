package com.meancat.aws.ec;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class ObjectCache {
    private static final Logger logger = LoggerFactory.getLogger(ObjectCache.class);

    MemcachedClient client;

    final static int CACHE_EXPIRATION = 3600;

    SerializingTranscoder transcoder = new SerializingTranscoder();

    public ObjectCache(MemcachedClient client) {
        this.client = client;
    }

    public <T> T get(String key) {
        logger.debug("get cache key '{}'", key);
        //noinspection unchecked
        return (T)client.get(key, transcoder);
    }

    public <T> Future<Boolean> set(String key, T object) {
        logger.debug("set {} to {}", key, object);
        return client.set(key, CACHE_EXPIRATION, object, transcoder);
    }

    public Future<Boolean> delete(String key) {
        logger.debug("Deleting cache key {}", key);
        return client.delete(key);
    }
}
