package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.repository.TransactionRepository;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by changmingxie on 10/26/15. 事务管理器，提供事务的获取、发起、提交、回滚，参与者的新增等等方法。
 */
public class TransactionManager {

    static final org.slf4j.Logger logger = LoggerFactory.getLogger(TransactionManager.class.getSimpleName());
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>(); // 当前线程事务队列


    private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2 + 1;

    private int threadQueueSize = 1024;

    private ExecutorService asyncTerminatorExecutorService = new ThreadPoolExecutor(threadPoolSize,
            threadPoolSize,
            0l,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(threadQueueSize), new ThreadPoolExecutor.AbortPolicy());

    private ExecutorService asyncSaveExecutorService = new ThreadPoolExecutor(threadPoolSize,
            threadPoolSize,
            0l,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(threadQueueSize * 2), new ThreadPoolExecutor.CallerRunsPolicy());

    private TransactionRepository transactionRepository;


    public TransactionManager() {
    }

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }
    // 发起根事务。该方法在调用方法类型为 ParticipantRole.ROOT 并且 事务处于 Try 阶段被调用
    public Transaction begin(Object uniqueIdentify) {
        Transaction transaction = new Transaction(uniqueIdentify, TransactionType.ROOT); // 创建 根事务

        //for performance tuning, at create stage do not persistent
//        transactionRepository.create(transaction); // 存储 事务
        registerTransaction(transaction); // 注册 事务
        return transaction;
    }
    // 发起根事务。该方法在调用方法类型为 ParticipantRole.ROOT 并且 事务处于 Try 阶段被调用
    public Transaction begin() {
        Transaction transaction = new Transaction(TransactionType.ROOT);
        //for performance tuning, at create stage do not persistent
//        transactionRepository.create(transaction);
        registerTransaction(transaction);
        return transaction;
    }
    // 传播发起分支事务。该方法在调用方法类型为 ParticipantRole.PROVIDER 并且 事务处于 Try 阶段被调用
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
        // 创建 分支事务
        Transaction transaction = new Transaction(transactionContext);

        //for performance tuning, at create stage do not persistent
//        transactionRepository.create(transaction); // 存储 事务
        registerTransaction(transaction); // 注册 事务
        return transaction;
    }
    // 传播获取分支事务。该方法在调用方法类型为 ParticipantRole.PROVIDER 并且 事务处于 Confirm / Cancel 阶段被调用
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid()); // 查询 事务

        if (transaction != null) {
            registerTransaction(transaction); // 注册 事务
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }
    // 添加参与者到事务
    public void enlistParticipant(Participant participant) {
        Transaction transaction = this.getCurrentTransaction(); // 获取 事务
        transaction.enlistParticipant(participant); // 添加参与者
        // 创建/更新 事务
        if (transaction.getVersion() == 0l) {
            // transaction.getVersion() is zero which means never persistent before, need call create to persistent.
            transactionRepository.create(transaction);
        } else {
            transactionRepository.update(transaction);
        }
    }
    // 提交事务
    public void commit(boolean asyncCommit) {
        // 获取 事务
        final Transaction transaction = getCurrentTransaction();
        // 设置 事务状态 为 CONFIRMING
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        // 更新 事务
        transactionRepository.update(transaction);

        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();

                asyncTerminatorExecutorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction); // 提交 事务
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("compensable transaction async submit confirm failed, recovery job will try to confirm later.", commitException.getCause());
                //throw new ConfirmingException(commitException);
            }
        } else {
            commitTransaction(transaction);
        }
    }

    //  取消事务，和 #commit() 方法基本类似。该方法在事务处于 Confirm / Cancel 阶段被调用
    public void rollback(boolean asyncRollback) {

        final Transaction transaction = getCurrentTransaction(); // 获取 事务
        transaction.changeStatus(TransactionStatus.CANCELLING); // 设置 事务状态 为 CANCELLING

        transactionRepository.update(transaction); // 更新 事务

        if (asyncRollback) {

            try {
                asyncTerminatorExecutorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                logger.warn("compensable transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancellingException(rollbackException);
            }
        } else {

            rollbackTransaction(transaction);
        }
    }


    private void commitTransaction(Transaction transaction) {
        try {
            transaction.commit(); // 提交 事务
            transactionRepository.delete(transaction); // 删除 事务
        } catch (Throwable commitException) {

            //try save updated transaction
            try {
                transactionRepository.update(transaction);
            } catch (Exception e) {
                //ignore any exception here
            }

            logger.warn("compensable transaction confirm failed, recovery job will try to confirm later.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    private void rollbackTransaction(Transaction transaction) {
        try {
            transaction.rollback(); // 提交 事务
            transactionRepository.delete(transaction); // 删除 事务
        } catch (Throwable rollbackException) {

            //try save updated transaction
            try {
                transactionRepository.update(transaction);
            } catch (Exception e) {
                //ignore any exception here
            }
            
            logger.warn("compensable transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }
    // 线程隔离
    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek();  // 获得头部元素。为什么获得队列头部元素呢？该元素即是上文调用 #registerTransaction(...) 注册到队列头部。
        }
        return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

    // 注册事务到当前线程事务队列。使用队列是因为支持多个的事务独立存在，后创建的事务先提交，类似 Spring 的org.springframework.transaction.annotation.Propagation.REQUIRES_NEW
    private void registerTransaction(Transaction transaction) {

        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }

        CURRENT.get().push(transaction);  // 添加到头部
    }
    // 将事务从当前线程事务队列移除，避免线程冲突
    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
                if (CURRENT.get().size() == 0) {
                    CURRENT.remove();
                }
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }


    public void changeStatus(TransactionStatus status) {
        changeStatus(status, false);
    }

    public void changeStatus(TransactionStatus status, boolean asyncSave) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.setStatus(status);

        if (asyncSave) {
            asyncSaveExecutorService.submit(new AsyncSaveTask(transaction));
        } else {
            transactionRepository.update(transaction);
        }
    }

    class AsyncSaveTask implements Runnable {

        private Transaction transaction;

        public AsyncSaveTask(Transaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public void run() {

            //only can be TRY_SUCCESS
            try {
                if (transaction != null && transaction.getStatus().equals(TransactionStatus.TRY_SUCCESS)) {

                    Transaction foundTransaction = transactionRepository.findByXid(transaction.getXid());

                    if (foundTransaction != null && foundTransaction.getStatus().equals(TransactionStatus.TRYING)) {
                        transactionRepository.update(transaction);
                    }
                }
            } catch (Exception e) {
                //ignore the exception
            }
        }
    }

}
