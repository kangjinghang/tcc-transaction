package org.mengyun.tcctransaction.repository;

import org.mengyun.tcctransaction.Transaction;

import javax.transaction.xa.Xid;
import java.io.Closeable;
import java.util.Date;

/**
 * Created by changmingxie on 11/12/15.
 */
public interface TransactionRepository extends Closeable {

    String getDomain();

    String getRootDomain();
    // 新增事务
    int create(Transaction transaction);
    // 更新事务
    int update(Transaction transaction);
    // 删除事务
    int delete(Transaction transaction);
    // 获取事务
    Transaction findByXid(Xid xid);

    Transaction findByRootXid(Xid xid);
    // 获取超过指定时间的事务集合
    Page<Transaction> findAllUnmodifiedSince(Date date, String offset, int pageSize);

    @Override
    default void close() {

    }
}
