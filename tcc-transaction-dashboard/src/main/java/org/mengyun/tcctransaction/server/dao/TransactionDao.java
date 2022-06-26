package org.mengyun.tcctransaction.server.dao;

import org.mengyun.tcctransaction.server.model.Page;
import org.mengyun.tcctransaction.server.vo.TransactionVo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by changming.xie on 9/7/16. 事务Dao 接口
 */
public interface TransactionDao extends AutoCloseable {

    void confirm(String globalTxId, String branchQualifier);

    void cancel(String globalTxId, String branchQualifier);

    void delete(String globalTxId, String branchQualifier);

    void restore(String globalTxId, String branchQualifier);
    // 重置事务重试次数
    void resetRetryCount(String globalTxId, String branchQualifier);

    String getDomain();
    // 获得事务 VO 数组
    default List<TransactionVo> find(Integer pageNum, int pageSize, String pattern) {
        return new ArrayList<>();
    }

    default int count(String domain) {
        return 0;
    }

    Page<TransactionVo> findTransactions(Integer pageNum, int pageSize);

    Page<TransactionVo> findDeletedTransactions(Integer pageNum, int pageSize);

    @Override
    default void close() throws Exception {
    }
}

