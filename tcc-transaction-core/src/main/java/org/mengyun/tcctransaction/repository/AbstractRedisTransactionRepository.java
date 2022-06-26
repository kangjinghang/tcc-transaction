package org.mengyun.tcctransaction.repository;

import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.repository.helper.RedisCommands;
import org.mengyun.tcctransaction.repository.helper.RedisHelper;
import org.mengyun.tcctransaction.repository.helper.TransactionStoreSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import javax.transaction.xa.Xid;
import java.util.*;
import java.util.stream.Collectors;
// redis 事务存储器抽象父类
public abstract class AbstractRedisTransactionRepository extends AbstractKVStoreTransactionRepository<Jedis> {

    protected boolean isSupportScan = true;

    @Override
    protected int doCreate(final Transaction transaction) {
        // 拼装 redis key，然后得到 RedisCommands
        try (RedisCommands commands = getRedisCommands(RedisHelper.getRedisKey(getDomain(), transaction.getXid()))) {
            Long statusCode = createByScriptCommand(commands, transaction);
            return statusCode.intValue();
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    @Override
    protected int doUpdate(final Transaction transaction) {
        // 拼装 redis key，然后得到 RedisCommands
        try (RedisCommands commands = getRedisCommands(RedisHelper.getRedisKey(getDomain(), transaction.getXid()))) {

            Long statusCode = updateByScriptCommand(commands, transaction);

            return statusCode.intValue();
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    @Override
    protected int doDelete(final Transaction transaction) {
        // 拼装 redis key，然后得到 RedisCommands
        try (RedisCommands commands = getRedisCommands(RedisHelper.getRedisKey(getDomain(), transaction.getXid()))) {

            Long result = commands.del(RedisHelper.getRedisKey(getDomain(), transaction.getXid()));

            return result.intValue();
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    @Override
    protected Transaction doFindOne(final Xid xid) {
        return doFind(getDomain(),xid);
    }

    @Override
    protected Transaction doFindRootOne(Xid xid) {
        return doFind(getRootDomain(),xid);
    }

    private Transaction doFind(String domain, Xid xid) {
        try (RedisCommands commands = getRedisCommands(RedisHelper.getRedisKey(domain, xid))) { // 拼装 redis key，然后得到 RedisCommands

            Long startTime = System.currentTimeMillis();
            // hgetAll
            Map<byte[], byte[]> content = commands.hgetAll(RedisHelper.getRedisKey(domain, xid));

            if (log.isDebugEnabled()) {
                log.debug("redis find cost time :" + (System.currentTimeMillis() - startTime));
            }

            if (content != null && content.size() > 0) { // 反序列化
                return TransactionStoreSerializer.deserialize(getSerializer(), content);
            }
            return null;
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }
    // 通过 lua 脚本新建事务
    protected Long createByScriptCommand(RedisCommands commands, Transaction transaction) {
        List<byte[]> params = new ArrayList<byte[]>();

        for (Map.Entry<byte[], byte[]> entry : TransactionStoreSerializer.serialize(getSerializer(), transaction)
                .entrySet()) {
            params.add(entry.getKey()); // key 和 value 都放到 list
            params.add(entry.getValue());
        }
        // commands 实现类通过 lua 脚本执行
        Object result = commands.eval(
                "if redis.call('exists', KEYS[1]) == 0 then redis.call('hmset', KEYS[1], unpack(ARGV)); return 1; end; return 0;"
                        .getBytes(),
                Arrays.asList(RedisHelper.getRedisKey(getDomain(), transaction.getXid())), // KEYS[1] 就是 redis key
                params);

        return (Long) result;
    }
    // 通过 lua 脚本更新事务
    protected Long updateByScriptCommand(RedisCommands commands, Transaction transaction) {

        transaction.setLastUpdateTime(new Date());
        transaction.setVersion(transaction.getVersion() + 1);

        List<byte[]> params = new ArrayList<byte[]>();

        for (Map.Entry<byte[], byte[]> entry : TransactionStoreSerializer.serialize(getSerializer(), transaction)
                .entrySet()) {
            params.add(entry.getKey());
            params.add(entry.getValue());
        }
        // commands 实现类通过 lua 脚本执行
        Object result = commands.eval(String.format(
                "if redis.call('hget',KEYS[1],'VERSION') == '%s' then redis.call('hmset', KEYS[1], unpack(ARGV)); return 1; end; return 0;",
                transaction.getVersion() - 1).getBytes(),
                Arrays.asList(RedisHelper.getRedisKey(
                        getDomain(),
                        transaction.getXid())),
                params);

        return (Long) result;
    }

    @Override
     List<Transaction> findTransactionsFromOneShard(Jedis shard, Set keys) {

        List<Transaction> list = null;
        // 得到 Pipeline
        Pipeline pipeline = shard.pipelined();
        // 批量执行 hgetAll
        for (final Object key : keys) {
            pipeline.hgetAll((byte[])key);
        }
        // 得到 Pipeline 执行结果
        List<Object> result = pipeline.syncAndReturnAll();

        list = new ArrayList<Transaction>();

        for (Object data : result) {

            if (data != null && data instanceof Map && ((Map<byte[], byte[]>) data).size() > 0) { // 结果不为空
                list.add(TransactionStoreSerializer.deserialize(getSerializer(), (Map<byte[], byte[]>) data)); // 反序列化
            } else if (data instanceof JedisMovedDataException) {
                // ignore the data, this case may happen under redis cluster.
                log.warn("ignore the data, this case may happen under redis cluster.", data);
            } else {
                log.warn("get transaction data failed. result is: " + data == null ? "null" : data.toString());
            }
        }

        return list;
    }

    @Override
    Page<byte[]> findKeysFromOneShard(Jedis shard, String currentCursor, int maxFindCount) {

        Page<byte[]> page = new Page<>();
        // 拼装 ScanParams，每次 scan maxFindCount 个 key
        ScanParams scanParams = RedisHelper.buildDefaultScanParams(getDomain() + "*", maxFindCount);
        // scan 执行
        ScanResult<String> scanResult = shard.scan(currentCursor, scanParams);
        // 设置返回值
        page.setData(scanResult.getResult().stream().map(v -> v.getBytes()).collect(Collectors.toList()));

        page.setAttachment(scanResult.getCursor());

        return page;
    }
    // 将 shardKey 包装成 RedisCommands 的子类实现对象
    protected abstract RedisCommands getRedisCommands(byte[] shardKey);
    // jedis 比较器
    public static class JedisComparator implements Comparator<Jedis> {

        @Override
        public int compare(Jedis jedis1, Jedis jedis2) {
            return String.format("%s:%s:%s", jedis1.getClient().getHost(), jedis1.getClient().getPort(), jedis1.getClient().getDB())
                    .compareTo(String.format("%s:%s:%s",
                            jedis2.getClient().getHost(),
                            jedis2.getClient().getPort(),
                            jedis2.getClient().getDB()));
        }
    }
}