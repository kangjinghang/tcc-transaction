package org.mengyun.tcctransaction.repository.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.Map;

/**
 * Created by changming.xie on 9/15/16.
 */
public class RedisHelper {

    static final Logger log = LoggerFactory.getLogger(RedisHelper.class.getSimpleName());

    public static int SCAN_COUNT = 30;
    public static String SCAN_TEST_PATTERN = "*";

    public static String REDIS_SCAN_INIT_CURSOR = ShardOffset.SCAN_INIT_CURSOR;
    // 创建事务的 Redis Key
    public static byte[] getRedisKey(String keyPrefix, Xid xid) {
        return new StringBuilder().append(keyPrefix).append(xid.toString()).toString().getBytes();
    }
    // 创建事务的 Redis Key
    public static byte[] getRedisKey(String keyPrefix, String globalTransactionId, String branchQualifier) {
        return new StringBuilder().append(keyPrefix)
                .append(globalTransactionId)
                .append(":")
                .append(branchQualifier)
                .toString()
                .getBytes();
    }

    public static <T> T execute(JedisPool jedisPool, JedisCallback<T> callback) {
        try (Jedis jedis = jedisPool.getResource()) { // jedis pool 获取连接
            return callback.doInJedis(jedis); // 将 jedis 传入到 callback 函数
        }
    }

    public static <T> T execute(ShardedJedisPool jedisPool, ShardedJedisCallback<T> callback) {
        try (ShardedJedis jedis = jedisPool.getResource()) {  // jedis pool 获取连接
            return callback.doInJedis(jedis); // 将 ShardedJedis 传入到 callback 函数
        }
    }

    public static ScanParams buildDefaultScanParams(String pattern, int count) {
        return new ScanParams().match(pattern).count(count); // scan 加入 pattern 和 count 参数
    }
    // 是否支持 scan 命令
    public static boolean isSupportScanCommand(Jedis jedis) {
        try {
            ScanParams scanParams = buildDefaultScanParams(SCAN_TEST_PATTERN, SCAN_COUNT);
            jedis.scan(REDIS_SCAN_INIT_CURSOR, scanParams);
        } catch (JedisDataException e) {
            log.error(e.getMessage(), e);
            log.info("Redis **NOT** support scan command"); // 抛出异常说明不支持
            return false;
        }

        log.info("Redis support scan command");
        return true;
    }
    // jedisPool 是否支持 scan 命令
    static public boolean isSupportScanCommand(JedisPool pool) {
        return execute(pool, jedis -> isSupportScanCommand(jedis));
    }
    // ShardedJedisPool 是否支持 scan 命令
    static public boolean isSupportScanCommand(ShardedJedisPool shardedJedisPool) {
        Collection<Jedis> allShards = shardedJedisPool.getResource().getAllShards(); // 获取所有分片

        for (Jedis jedis : allShards) {
            try {
                jedis.connect();
                if (!isSupportScanCommand(jedis)) { // 逐个判断
                    return false;
                }
            } finally {
                if (jedis.isConnected()) {
                    jedis.disconnect(); // 断连
                }
            }
        }

        return true;
    }
    // jedis cluster 是否支持 scan 命令
    static public boolean isSupportScanCommand(JedisCluster jedisCluster) {
        Map<String, JedisPool> jedisPoolMap = jedisCluster.getClusterNodes(); // 获取所有 Node

        for (Map.Entry<String, JedisPool> entry : jedisPoolMap.entrySet()) {
            if (!isSupportScanCommand(entry.getValue())) {
                return false;
            }
        }

        return true;
    }
    // 拼装 scan 参数
    public static ScanParams scanArgs(String pattern, int count) {
        return new ScanParams().match(pattern).count(count);
    }
}