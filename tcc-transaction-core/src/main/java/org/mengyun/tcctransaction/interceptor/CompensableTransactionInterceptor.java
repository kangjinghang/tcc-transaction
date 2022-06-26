package org.mengyun.tcctransaction.interceptor;

import com.alibaba.fastjson.JSON;
import org.mengyun.tcctransaction.IllegalTransactionStatusException;
import org.mengyun.tcctransaction.NoExistedTransactionException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.ParticipantStatus;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 10/30/15. 可补偿事务拦截器。通常是 发起和参与 分布式事务的各个服务标注了 @Compensable 的 Try 方法。主要是创建根事务/分支事务 Transaction 放到本地线程事务队列里、提交/回滚 Transaction
 */
public class CompensableTransactionInterceptor {

    static final Logger logger = LoggerFactory.getLogger(CompensableTransactionInterceptor.class.getSimpleName());

    private TransactionManager transactionManager;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Object interceptCompensableMethod(TransactionMethodJoinPoint pjp) throws Throwable {
        // 获得 事务上下文。第一次开启分布式事务的时候，还没有当前事务，返回 null
        Transaction transaction = transactionManager.getCurrentTransaction();
        CompensableMethodContext compensableMethodContext = new CompensableMethodContext(pjp, transaction); // 创建 CompensableMethodContext 实例

        // if method is @Compensable and no transaction context and no transaction, then root
        // else if method is @Compensable and has transaction context and no transaction ,then provider
        switch (compensableMethodContext.getParticipantRole()) {
            case ROOT: // 发起分布式事务的服务 Try 方法
                return rootMethodProceed(compensableMethodContext);
            case PROVIDER: // 除了发起分布式事务的服务之外的其他参与方 Try 方法
                return providerMethodProceed(compensableMethodContext);
            default: // 为 ParticipantRole.NORMAL 时，执行方法原逻辑，不进行事务处理
                return compensableMethodContext.proceed();
        }
    }
    // 发起 TCC 整体流程。发起分布式事务的服务 Try 方法会走到这里，创建一个本地的根事务 Transaction
    private Object rootMethodProceed(CompensableMethodContext compensableMethodContext) throws Throwable {

        Object returnValue = null;

        Transaction transaction = null;

        boolean asyncConfirm = compensableMethodContext.getAnnotation().asyncConfirm();

        boolean asyncCancel = compensableMethodContext.getAnnotation().asyncCancel();

        try {
            // 发起根事务，TCC Try 阶段开始，创建一个本地的【根】事务 Transaction，并注册到本地线程的事务队列里
            transaction = transactionManager.begin(compensableMethodContext.getUniqueIdentity());
            // 走第二个 资源拦截器，创建所有的参与者加到 list 里
            try {
                returnValue = compensableMethodContext.proceed(); // 执行方法原逻辑( 即 Try 逻辑 )
            } catch (Throwable tryingException) {
                // 当原逻辑执行异常时，TCC Try 阶段失败，调用 rollback。进入 TCC Cancel 阶段，回滚事务
                transactionManager.rollback(asyncCancel); // 回滚事务

                throw tryingException;
            }
            // 提交事务。当原逻辑执行成功时，TCC Try 阶段成功了，下面进入 TCC Confirm 阶段，提交事务
            transactionManager.commit(asyncConfirm);

        } finally {
            transactionManager.cleanAfterCompletion(transaction); // 将事务从当前线程事务队列移除
        }

        return returnValue;
    }
    // 服务提供者（除了发起分布式事务的服务之外的其他参与方）参与 TCC 整体流程。
    private Object providerMethodProceed(CompensableMethodContext compensableMethodContext) throws Throwable {
        // 本地发起远程服务 TCC confirm / cancel 阶段，调用相同方法进行事务的提交或回滚。远程服务的 CompensableTransactionInterceptor 会根据事务的状态是 CONFIRMING / CANCELLING 来调用对应方法
        Transaction transaction = null;

        boolean asyncConfirm = compensableMethodContext.getAnnotation().asyncConfirm();

        boolean asyncCancel = compensableMethodContext.getAnnotation().asyncCancel();

        try {

            switch (TransactionStatus.valueOf(compensableMethodContext.getTransactionContext().getStatus())) {
                case TRYING: // Try 阶段会传播发起分支事务，就是根据上游服务透传过来的 TransactionContext 创建一个本地的分支事务 Transaction，并注册到本地线程的事务队列里
                    transaction = transactionManager.propagationNewBegin(compensableMethodContext.getTransactionContext());  // 传播发起分支事务
                    // 为什么要传播发起分支事务？在根事务进行 Confirm / Cancel 时，调用根事务上的参与者们提交或回滚事务时，进行远程服务方法调用的参与者，可以通过自己的事务编号关联上传播的分支事务( 两者的事务编号相等 )，进行事务的提交或回滚。
                    Object result = null;

                    try {
                        result = compensableMethodContext.proceed(); // 除了发起分布式事务的服务之外的其他参与方执行 Try 方法

                        //TODO: need tuning here, async change the status to tuning the invoke chain performance
                        //transactionManager.changeStatus(TransactionStatus.TRY_SUCCESS, asyncSave);
                        transactionManager.changeStatus(TransactionStatus.TRY_SUCCESS, true);
                    } catch (Throwable e) {
                        transactionManager.changeStatus(TransactionStatus.TRY_FAILED);
                        throw e;
                    }
                    // 这里是 return 了原始 Try 方法的返回值
                    return result;

                case CONFIRMING: // 发起方通知进行提交，查到发起方传播过来的存在本地的分支事务，然后提交就变成了真正执行 @Compensable 注解标注的 confirmMethod 方法了
                    try {
                        transaction = transactionManager.propagationExistBegin(compensableMethodContext.getTransactionContext());  // 获取发起方传播过来的存在本地的分支事务
                        transactionManager.commit(asyncConfirm);  // 提交事务
                    } catch (NoExistedTransactionException excepton) {
                        //the transaction has been commit,ignore it.
                        logger.warn("no existed transaction found at CONFIRMING stage, will ignore and confirm automatically. transaction:" + JSON.toJSONString(transaction));
                    }
                    break;
                case CANCELLING: // 发起方通知进行提交，查到发起方传播过来的存在本地的分支事务，然后提交就变成了真正执行 @Compensable 注解标注的 cancelMethod 方法了

                    try {

                        //The transaction' status of this branch transaction, passed from consumer side.
                        int transactionStatusFromConsumer = compensableMethodContext.getTransactionContext().getParticipantStatus();
                        // 传播获取分支事务
                        transaction = transactionManager.propagationExistBegin(compensableMethodContext.getTransactionContext());

                        // Only if transaction's status is at TRY_SUCCESS、TRY_FAILED、CANCELLING stage we can call rollback.
                        // If transactionStatusFromConsumer is TRY_SUCCESS, no mate current transaction is TRYING or not, also can rollback.
                        // transaction's status is TRYING while transactionStatusFromConsumer is TRY_SUCCESS may happen when transaction's changeStatus is async.
                        if (transaction.getStatus().equals(TransactionStatus.TRY_SUCCESS)
                                || transaction.getStatus().equals(TransactionStatus.TRY_FAILED)
                                || transaction.getStatus().equals(TransactionStatus.CANCELLING)
                                || transactionStatusFromConsumer == ParticipantStatus.TRY_SUCCESS.getId()) {
                            transactionManager.rollback(asyncCancel); // 回滚事务
                        } else {
                            //in this case, transaction's Status is TRYING and transactionStatusFromConsumer is TRY_FAILED
                            // this may happen if timeout exception throws during rpc call.
                            throw new IllegalTransactionStatusException("Branch transaction status is TRYING, cannot rollback directly, waiting for recovery job to rollback.");
                        }

                    } catch (NoExistedTransactionException exception) {
                        //the transaction has been rollback,ignore it.
                        logger.info("no existed transaction found at CANCELLING stage, will ignore and cancel automatically. transaction:" + JSON.toJSONString(transaction));
                    }
                    break;
            }

        } finally {
            transactionManager.cleanAfterCompletion(transaction); // 将事务从当前线程事务队列移除
        }
        // 当事务处于 TransactionStatus.CONFIRMING / TransactionStatus.CANCELLING 时，返回空值
        Method method = compensableMethodContext.getMethod();
        // 为什么返回空值？Confirm / Cancel 相关方法，是通过 AOP 切面调用，只调用，不处理返回值，但是又不能没有返回值，因此直接返回空
        return ReflectionUtils.getNullValue(method.getReturnType());
    }
}
