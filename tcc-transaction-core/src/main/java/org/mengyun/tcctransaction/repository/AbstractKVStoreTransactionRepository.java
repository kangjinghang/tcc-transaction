package org.mengyun.tcctransaction.repository;

import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.repository.helper.ShardHolder;
import org.mengyun.tcctransaction.repository.helper.ShardOffset;
import org.mengyun.tcctransaction.serializer.RegisterableKryoTransactionSerializer;
import org.mengyun.tcctransaction.serializer.TransactionSerializer;
import org.mengyun.tcctransaction.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractKVStoreTransactionRepository<T> extends AbstractTransactionRepository {

    static final Logger log = LoggerFactory.getLogger(AbstractKVStoreTransactionRepository.class.getSimpleName());
    // key 前缀
    private String domain;

    private String rootDomain;
    // 序列化器
    private TransactionSerializer serializer = new RegisterableKryoTransactionSerializer();

    @Override
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    @Override
    public String getRootDomain() {
        return rootDomain;
    }

    public void setRootDomain(String rootDomain) {
        this.rootDomain = rootDomain;
    }

    public void setSerializer(TransactionSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected Page<Transaction> doFindAllUnmodifiedSince(Date date, String offset, int pageSize) {

        List<Transaction> fetchedTransactions = new ArrayList<>();

        String tryFetchOffset = offset;

        int haveFetchedCount = 0;

        do {

            Page<Transaction> page = doFindAll(tryFetchOffset, pageSize - haveFetchedCount);

            tryFetchOffset = page.getNextOffset();

            for (Transaction transaction : page.getData()) {
                if (transaction.getLastUpdateTime().compareTo(date) < 0) {
                    fetchedTransactions.add(transaction);
                }
            }

            haveFetchedCount += page.getData().size();
            // 最后一页或者查到了 pageSize 的数据就退出循环
            if (page.getData().size() <= 0 || haveFetchedCount >= pageSize) {
                break;
            }
        } while (true);

        return new Page<Transaction>(tryFetchOffset, fetchedTransactions);
    }

    /*
     * offset 格式: shardIndex:cursor,eg = 0:0,1:0。maxFindCount 是每页查找的个数最大值
     * */
    protected Page<Transaction> doFindAll(String offset, int maxFindCount) {

        ShardOffset currentShardOffset = new ShardOffset(offset);

        ShardOffset nextShardOffset = new ShardOffset();

        Page<Transaction> page = new Page<Transaction>();

        try (ShardHolder shardHolder = getShardHolder()) {

            List<T> allShards = shardHolder.getAllShards(); // 获取所有分片
            // 从
            List<Transaction> transactions = findTransactionsFromShards(allShards, currentShardOffset, nextShardOffset, maxFindCount);

            page.setNextOffset(nextShardOffset.toString());
            page.setData(transactions);


            return page;

        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }
    // 在所有分片中从 currentShardOffset 的分片开始查找，直到 keyPage 的 data 不为空
    private List<Transaction> findTransactionsFromShards(final List<T> allShards, ShardOffset currentShardOffset, ShardOffset nextShardOffset, int maxFindCount) {

        List<Transaction> transactions = new ArrayList<>();

        Set<byte[]> allKeySet = new HashSet<>();

        int currentShardIndex = currentShardOffset.getShardIndex();
        String currentCursor = currentShardOffset.getCursor();
        String nextCursor = null;


        while (currentShardIndex < allShards.size()) { // 从 currentShardIndex 开始，循环到所有分片为止

            T currentShard = allShards.get(currentShardIndex);
            // 先从 currentShard 分片的 currentCursor 开始查找之后的所有 key
            Page<byte[]> keyPage = findKeysFromOneShard(currentShard, currentCursor, maxFindCount);
            List<byte[]> keys = keyPage.getData();

            if (keys.size() > 0) { // keys 的数量大于 0
                // 根据 keys 从 currentShard 查找事务列表
                List<Transaction> currentTransactions = findTransactionsFromOneShard(currentShard, new HashSet<>(keys));

                if (CollectionUtils.isEmpty(currentTransactions)) {
                    // ignore, maybe the keys are recovered by other threads!
                    log.info("no transaction found while key size:" + keys.size());
                }

                transactions.addAll(currentTransactions);
            }

            nextCursor = (String) keyPage.getAttachment(); // 下一页的游标
            allKeySet.addAll(keyPage.getData()); // 当前页的 key 加入到 allKeySet 中

            if (CollectionUtils.isEmpty(allKeySet)) {
                if (ShardOffset.SCAN_INIT_CURSOR.equals(nextCursor)) { // 到这个分片的末尾了
                    //end of the jedis, try next jedis again.
                    currentShardIndex += 1; // 开始查找下一个分片
                    currentCursor = ShardOffset.SCAN_INIT_CURSOR;

                    currentShardOffset.setShardIndex(currentShardIndex); // 分片编号初始化
                    currentShardOffset.setCursor(currentCursor); // 查找游标初始化
                } else {
                    //keys is empty, do while again, start from last cursor position
                    currentCursor = nextCursor; // 从结束的地方继续查找
                }
            } else {
                break;
            }
        }

        if (CollectionUtils.isEmpty(allKeySet)) { // 查找到最后一个分片也都是空的
            nextShardOffset.setShardIndex(currentShardIndex);
            nextShardOffset.setCursor(currentCursor);
        } else {
            if (ShardOffset.SCAN_INIT_CURSOR.equals(nextCursor)) { // 当前这个分片查完了
                nextShardOffset.setShardIndex(currentShardIndex + 1); // 分片编号 + 1
                nextShardOffset.setCursor(ShardOffset.SCAN_INIT_CURSOR); // 从下一个分片起始位置开始查
            } else {
                nextShardOffset.setShardIndex(currentShardIndex);
                nextShardOffset.setCursor(nextCursor);
            }
        }

        return transactions;
    }

    abstract  List<Transaction> findTransactionsFromOneShard(T shard, Set keys);

    abstract  Page findKeysFromOneShard(T shard, String currentCursor, int maxFindCount);

    protected abstract ShardHolder<T> getShardHolder();

    public TransactionSerializer getSerializer() {
        return serializer;
    }
}
