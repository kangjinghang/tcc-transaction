package org.mengyun.tcctransaction.repository.helper;

import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.List;
import java.util.Map;
// jedis cluster 实现
public class JedisClusterCommands implements RedisCommands {

    private JedisCluster jedisCluster;

    public JedisClusterCommands(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public Object eval(byte[] scripts, List<byte[]> keys, List<byte[]> args) {
        return this.jedisCluster.eval(scripts, keys, args);
    }

    @Override
    public Long del(byte[] key) {
        return this.jedisCluster.del(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return this.jedisCluster.hgetAll(key);
    }

    @Override
    public void hset(byte[] key, byte[] field, byte[] value) {
        this.jedisCluster.hset(key, field, value);
    }

    @Override
    public void hdel(byte[] key, byte[] field) {
        this.jedisCluster.hdel(key, field);
    }

    @Override
    public void expire(byte[] key, int expireTime) {
        this.expire(key, expireTime);
    }

    @Override
    public List<Object> executePipelined(CommandCallback<List<Object>> commandCallback) {
        return commandCallback.execute(this);
    }

    @Override
    public void close() throws IOException {

    }
}
