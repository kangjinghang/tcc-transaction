package org.mengyun.tcctransaction.repository;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.junit.Test;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import redis.clients.jedis.JedisPool;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * RedisTransactionRepositoryTest
 *
 * @author <a href="kangjinghang@xueqiu.com">kangjinghang</a>
 * @date 2022-06-24
 * @since 1.0.0
 */
public class RedisTransactionRepositoryTest extends AbstractTransactionRepositoryTest {

    @Override
    TransactionRepository doCreateTransactionRepository() {
        final RedisTransactionRepository redisTransactionRepository = new RedisTransactionRepository();
        JedisPool jedisPool = new JedisPool();
        redisTransactionRepository.setJedisPool(jedisPool);
        redisTransactionRepository.setDomain("kang-tcc");
        return redisTransactionRepository;
    }

    @Test
    public void testGetDomain() {
        final String domain = repository.getDomain();
        System.out.println(domain);
    }

    @Test
    public void testGetRootDomain() {
        final String rootDomain = repository.getRootDomain();
        System.out.println(rootDomain);
    }

    @Test
    public void testCreate() {
        System.out.println("rootXid = " + rootXid);
        System.out.println("xid = " + xid);
        // rootXid = 4b7115ef-bb62-3623-acff-0ace408161ec:7d12bde4-ad1a-3ab2-9da9-6cabf2cfc5ca
        // xid 作为文件名
        // xid = 9fa3eab2-14eb-3b22-915a-858f83db626f:0c6c35e6-cf89-31f2-a7c0-7bf9528f7ceb
        TransactionContext context = new TransactionContext(rootXid, xid, TransactionStatus.TRYING.getId());
        Transaction transaction = new Transaction(context);
        final int count = repository.create(transaction);
        System.out.println("count = " + count);
    }

    @Test
    public void testFindByXid() {
        System.out.println("xid = " + xid);
        // xid = 9fa3eab2-14eb-3b22-915a-858f83db626f:0c6c35e6-cf89-31f2-a7c0-7bf9528f7ceb
        final Transaction transaction = repository.findByXid(xid);
        System.out.println(JSON.toJSONString(transaction, SerializerFeature.PrettyFormat));
    }

    @Test
    public void testUpdate() {
        System.out.println("xid = " + xid);
        // xid = 9fa3eab2-14eb-3b22-915a-858f83db626f:0c6c35e6-cf89-31f2-a7c0-7bf9528f7ceb
        Transaction transaction = repository.findByXid(xid);
        if (transaction != null) {
            transaction.commit();
            repository.update(transaction);
            transaction = repository.findByXid(xid);
            System.out.println(JSON.toJSONString(transaction, SerializerFeature.PrettyFormat));
        } else {
            System.out.println("transaction not found to update");
        }
    }

    @Test
    public void testFindByRootXid() {
        System.out.println("xid = " + xid);
        // xid = 9fa3eab2-14eb-3b22-915a-858f83db626f:0c6c35e6-cf89-31f2-a7c0-7bf9528f7ceb
        final Transaction transaction = repository.findByXid(xid);
        System.out.println(JSON.toJSONString(transaction, SerializerFeature.PrettyFormat));
    }

    @Test
    public void testFindAllUnmodifiedSince() {
        LocalDateTime begin = LocalDateTime.now().plusDays(1);
        final long epochMilli = begin.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        final Date date = new Date(epochMilli);
        System.out.println(date);
        final Page<Transaction> page = repository.findAllUnmodifiedSince(date, "0::0", 10);
        page.getData().forEach(e -> System.out.println(JSON.toJSONString(e, SerializerFeature.PrettyFormat)));
    }

    @Test
    public void testDelete() {
        System.out.println("xid = " + xid);
        final Transaction transaction = repository.findByXid(xid);
        if (transaction != null) {
            final int delete = repository.delete(transaction);
            System.out.println("delete = " + delete);
        } else {
            System.out.println("transaction not found to delete");
        }
    }

}
