package org.mengyun.tcctransaction.repository.helper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.List;
import java.util.Map;
// jedis 实现
public class JedisCommands implements RedisCommands {

    private Jedis jedis = null;

    public JedisCommands(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public Object eval(byte[] scripts, List<byte[]> keys, List<byte[]> args) {
        return this.jedis.eval(scripts, keys, args);
    }

    @Override
    public Long del(byte[] key) {
        return this.jedis.del(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return this.jedis.hgetAll(key);
    }

    @Override
    public void hset(byte[] key, byte[] field, byte[] value) {
        this.jedis.hset(key, field, value);
    }

    @Override
    public void hdel(byte[] key, byte[] field) {
        this.jedis.hdel(key, field);
    }

    @Override
    public void expire(byte[] key, int expireTime) {
        this.jedis.expire(key, expireTime);
    }

    @Override
    public List<Object> executePipelined(CommandCallback commandCallback) {
        // 获取 Pipeline
        Pipeline pipeline = jedis.pipelined();
        // 如果是 pipeline执行，需要封装成 Pipeline 命令（PipelineCommands），然后执行回调函数
        commandCallback.execute(new PipelineCommands(pipeline)); // 实现的时候通过 PipelineCommands  执行
        // 阻塞执行，返回
        return pipeline.syncAndReturnAll();
    }

    @Override
    public void close() throws IOException {
        if (jedis != null) {
            jedis.close();
        }
    }
}
