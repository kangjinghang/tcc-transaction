package org.mengyun.tcctransaction.repository.helper;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ShardJedisCommands implements RedisCommands {
    // 分片 jedis
    private ShardedJedis shardedJedis;
    // 分片 key
    private byte[] shardKey;

    public ShardJedisCommands(ShardedJedis shardedJedis) {
        this(shardedJedis, null);
    }

    public ShardJedisCommands(ShardedJedis shardedJedis, byte[] shardKey) {
        this.shardedJedis = shardedJedis;
        this.shardKey = shardKey;
    }

    @Override
    public Object eval(byte[] scripts, List<byte[]> keys, List<byte[]> args) {
        if (shardKey != null) {
            return this.shardedJedis.getShard(shardKey).eval(scripts, keys, args); // 先得到 shard
        } else {
            throw new UnsupportedOperationException("no shardKey, cann't call eval");
        }
    }

    @Override
    public Long del(byte[] key) {
        return this.shardedJedis.del(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return this.shardedJedis.hgetAll(key);
    }

    @Override
    public void hset(byte[] key, byte[] field, byte[] value) {
        this.shardedJedis.hset(key, field, value);
    }

    @Override
    public void hdel(byte[] key, byte[] field) {
        this.shardedJedis.hdel(key, field);
    }

    @Override
    public void expire(byte[] key, int expireTime) {
        this.shardedJedis.expire(key, expireTime);
    }

    @Override
    public List<Object> executePipelined(CommandCallback commandCallback) {

        ShardedJedisPipeline shardedJedisPipeline = this.shardedJedis.pipelined();
        // 封装成 ShardedPipelineCommand
        commandCallback.execute(new ShardedPipelineCommand(shardedJedisPipeline));

        return shardedJedisPipeline.syncAndReturnAll();

    }

    @Override
    public void close() throws IOException {
        if (this.shardedJedis != null) {
            this.shardedJedis.close();
        }
    }
}
