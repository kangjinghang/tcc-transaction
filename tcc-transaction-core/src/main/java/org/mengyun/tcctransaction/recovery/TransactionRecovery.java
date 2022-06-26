package org.mengyun.tcctransaction.recovery;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionOptimisticLockException;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.repository.LocalStorable;
import org.mengyun.tcctransaction.repository.Page;
import org.mengyun.tcctransaction.repository.SentinelTransactionRepository;
import org.mengyun.tcctransaction.repository.TransactionRepository;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.mengyun.tcctransaction.api.TransactionStatus.CANCELLING;
import static org.mengyun.tcctransaction.api.TransactionStatus.CONFIRMING;


/**
 * Created by changmingxie on 11/10/15. 事务恢复逻辑
 */
public class TransactionRecovery {

    public static final int CONCURRENT_RECOVERY_TIMEOUT = 60;
    // 默认打印错误日志的最大次数
    public static final int MAX_ERROR_COUNT_SHREDHOLD = 15;

    static final Logger logger = LoggerFactory.getLogger(TransactionRecovery.class.getSimpleName());

    static volatile ExecutorService recoveryExecutorService = null;

    private TransactionConfigurator transactionConfigurator;

    private AtomicInteger triggerMaxRetryPrintCount = new AtomicInteger();

    private AtomicInteger recoveryFailedPrintCount = new AtomicInteger();
    // 打印错误日志的最大次数 15 次
    private volatile int logMaxPrintCount = MAX_ERROR_COUNT_SHREDHOLD;

    private Lock logSync = new ReentrantLock();

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }
    // 启动恢复事务逻辑，每次定时任务调度一次
    public void startRecover() {

        ensureRecoveryInitialized();

        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();

        if (transactionRepository instanceof SentinelTransactionRepository) {

            SentinelTransactionRepository sentinelTransactionRepository = (SentinelTransactionRepository) transactionRepository;

            if (!sentinelTransactionRepository.getSentinelController().degrade()) {
                startRecover(sentinelTransactionRepository.getWorkTransactionRepository());
            }

            startRecover(sentinelTransactionRepository.getDegradedTransactionRepository());

        } else {
            startRecover(transactionRepository);
        }
    }

    // 每次定时任务调度一次
    public void startRecover(TransactionRepository transactionRepository) {
        // 本地事务存储器用本地 ReentrantLock 锁，否则用分布式锁 RedissonRecoveryLock
        Lock recoveryLock = transactionRepository instanceof LocalStorable ? RecoveryLock.DEFAULT_LOCK : transactionConfigurator.getRecoveryLock();

        if (recoveryLock.tryLock()) { // 上锁
            try {

                String offset = null;

                int totalCount = 0;
                do {
                    // 分页加载异常事务集合
                    Page<Transaction> page = loadErrorTransactionsByPage(transactionRepository, offset);

                    if (page.getData().size() > 0) {
                        concurrentRecoveryErrorTransactions(transactionRepository, page.getData()); // 并发恢复异常事务集合
                        offset = page.getNextOffset();
                        totalCount += page.getData().size();
                    } else {
                        break;
                    }
                } while (true);

                logger.debug(String.format("total recovery count %d from repository:%s", totalCount, transactionRepository.getClass().getName()));
            } catch (Throwable e) {
                logger.error(String.format("recovery failed from repository:%s.", transactionRepository.getClass().getName()), e);
            } finally {
                recoveryLock.unlock();
            }
        }
    }
    // 分页加载异常事务集合
    private Page<Transaction> loadErrorTransactionsByPage(TransactionRepository transactionRepository, String offset) {

        long currentTimeInMillis = Instant.now().toEpochMilli();

        RecoverFrequency recoverFrequency = transactionConfigurator.getRecoverFrequency();
        // 异常事务的定义：当前时间超过 - 事务变更时间( 最后执行时间 ) >= 事务恢复间隔( recoverFrequency#getRecoverDuration() )。这里有一点要注意，已完成的事务会从事务存储器删除。
        return transactionRepository.findAllUnmodifiedSince(new Date(currentTimeInMillis - recoverFrequency.getRecoverDuration() * 1000), offset, recoverFrequency.getFetchPageSize());
    }

    // 并发恢复 【这一页的】异常事务集合
    private void concurrentRecoveryErrorTransactions(TransactionRepository transactionRepository, List<Transaction> transactions) throws InterruptedException, ExecutionException {
        // 每一页在恢复的时候都会初始化一次
        initLogStatistics();

        List<RecoverTask> tasks = new ArrayList<>();
        for (Transaction transaction : transactions) {
            tasks.add(new RecoverTask(transactionRepository, transaction));
        }

        List<Future<Void>> futures = recoveryExecutorService.invokeAll(tasks, CONCURRENT_RECOVERY_TIMEOUT, TimeUnit.SECONDS);

        for (Future future : futures) {
            future.get();
        }
    }

    private void recoverErrorTransactions(TransactionRepository transactionRepository, List<Transaction> transactions) {

        initLogStatistics();

        for (Transaction transaction : transactions) {
            recoverErrorTransaction(transactionRepository, transaction);
        }
    }
    // 调度任务按页查询出异常任务，每个异常任务封装成一个task，这是真正的异常任务恢复逻辑
    private void recoverErrorTransaction(TransactionRepository transactionRepository, Transaction transaction) {
        // 当前事务超过最大重试次数 30
        if (transaction.getRetriedCount() > transactionConfigurator.getRecoverFrequency().getMaxRetryCount()) {

            logSync.lock();
            try {
                if (triggerMaxRetryPrintCount.get() < logMaxPrintCount) { // 当前事务重试次数不到 15 次，每次打印错误日志
                    logger.error(String.format(
                            "recover failed with max retry count,will not try again. txid:%s, status:%s,retried count:%d,transaction content:%s",
                            transaction.getXid(),
                            transaction.getStatus().getId(),
                            transaction.getRetriedCount(),
                            JSON.toJSONString(transaction)));
                    triggerMaxRetryPrintCount.incrementAndGet(); // 打印错误日志数+1
                } else if (triggerMaxRetryPrintCount.get() == logMaxPrintCount) { // 超过 15 次后，不再打印错误日志了
                    logger.error("Too many transaction's retried count max then MaxRetryCount during one page transactions recover process , will not print errors again!");
                }

            } finally {
                logSync.unlock();
            }
            // 返回了
            return;
        }

        try {
            // 根事务
            if (transaction.getTransactionType().equals(TransactionType.ROOT)) {

                switch (transaction.getStatus()) {
                    case CONFIRMING:
                        commitTransaction(transactionRepository, transaction);
                        break;
                    case CANCELLING:
                        rollbackTransaction(transactionRepository, transaction);
                        break;
                    default:
                        //the transaction status is TRYING, ignore it.
                        break;

                }

            } else {
                // 分支事务
                //transaction type is BRANCH
                switch (transaction.getStatus()) {
                    case CONFIRMING:
                        commitTransaction(transactionRepository, transaction);
                        break;
                    case CANCELLING:
                    case TRY_FAILED:
                        rollbackTransaction(transactionRepository, transaction);
                        break;
                    case TRY_SUCCESS:

                        if(transactionRepository.getRootDomain() == null) {
                            break;
                        }

                        //check the root transaction
                        Transaction rootTransaction = transactionRepository.findByRootXid(transaction.getRootXid());

                        if (rootTransaction == null) { // 根事务被删了，分支事务也要回滚
                            // In this case means the root transaction is already rollback.
                            // Need cancel this branch transaction.
                            rollbackTransaction(transactionRepository, transaction);
                        } else { // 根事务还没被删（说明根事务在提交/回滚），根据根事务的状态，采取相同的提交/回滚操作
                            switch (rootTransaction.getStatus()) {
                                case CONFIRMING:
                                    commitTransaction(transactionRepository, transaction);
                                    break;
                                case CANCELLING:
                                    rollbackTransaction(transactionRepository, transaction);
                                    break;
                                default:
                                    break;
                            }
                        }
                        break;
                    default:
                        // the transaction status is TRYING, ignore it.
                        break;
                }

            }

        } catch (Throwable throwable) {

            if (throwable instanceof TransactionOptimisticLockException
                    || ExceptionUtils.getRootCause(throwable) instanceof TransactionOptimisticLockException) {

                logger.warn(String.format(
                        "optimisticLockException happened while recover. txid:%s, status:%d,retried count:%d",
                        transaction.getXid(),
                        transaction.getStatus().getId(),
                        transaction.getRetriedCount()));
            } else {

                logSync.lock();
                try {
                    if (recoveryFailedPrintCount.get() < logMaxPrintCount) { // 这一页的异常事务恢复时发送异常的次数不到 15 次，每次打印错误日志
                        logger.error(String.format("recover failed, txid:%s, status:%s,retried count:%d,transaction content:%s",
                                transaction.getXid(),
                                transaction.getStatus().getId(),
                                transaction.getRetriedCount(),
                                JSON.toJSONString(transaction)), throwable);
                        recoveryFailedPrintCount.incrementAndGet();
                    } else if (recoveryFailedPrintCount.get() == logMaxPrintCount) {  // 这一页的异常事务恢复时发送异常的次数达到了 15 次，以后不打印了
                        logger.error("Too many transaction's recover error during one page transactions recover process , will not print errors again!");
                    }
                } finally {
                    logSync.unlock();
                }
            }
        }
    }

    private void rollbackTransaction(TransactionRepository transactionRepository, Transaction transaction) {
        transaction.setRetriedCount(transaction.getRetriedCount() + 1); // 增加重试次数
        transaction.setStatus(CANCELLING);
        transactionRepository.update(transaction);
        transaction.rollback();
        transactionRepository.delete(transaction);
    }

    private void commitTransaction(TransactionRepository transactionRepository, Transaction transaction) {
        transaction.setRetriedCount(transaction.getRetriedCount() + 1); // 增加重试次数
        transaction.setStatus(CONFIRMING);
        transactionRepository.update(transaction);
        transaction.commit();
        transactionRepository.delete(transaction);
    }


    private void ensureRecoveryInitialized() {

        if (recoveryExecutorService == null) {
            synchronized (TransactionRecovery.class) {
                if (recoveryExecutorService == null) {

                    recoveryExecutorService = Executors.newFixedThreadPool(transactionConfigurator.getRecoverFrequency().getConcurrentRecoveryThreadCount());
                    // 如果每页个数的一半（默认250）超过了 默认打印错误日志的最大次数 15 ，就用 15，否则为 每页个数的一半
                    logMaxPrintCount = transactionConfigurator.getRecoverFrequency().getFetchPageSize() / 2
                            > MAX_ERROR_COUNT_SHREDHOLD ?
                            MAX_ERROR_COUNT_SHREDHOLD : transactionConfigurator.getRecoverFrequency().getFetchPageSize() / 2;


                }
            }
        }
    }

    private void initLogStatistics() {
        triggerMaxRetryPrintCount.set(0);
        recoveryFailedPrintCount.set(0);
    }

    // 恢复异常事务任务
    class RecoverTask implements Callable<Void> {

        TransactionRepository transactionRepository;
        Transaction transaction;

        public RecoverTask(TransactionRepository transactionRepository, Transaction transaction) {
            this.transactionRepository = transactionRepository;
            this.transaction = transaction;
        }

        @Override
        public Void call() throws Exception {
            recoverErrorTransaction(transactionRepository, transaction); // 恢复异常事务
            return null;
        }
    }

}
