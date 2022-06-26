package org.mengyun.tcctransaction.interceptor;

import org.mengyun.tcctransaction.InvocationContext;
import org.mengyun.tcctransaction.Participant;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.*;
import org.mengyun.tcctransaction.support.FactoryBuilder;

/**
 * Created by changmingxie on 11/8/15. 资源协调者拦截器。第二个拦截器。切入点是标注了 @Compensable 注解 || 参数列表中有 TransactionContext 类型的，其实就是远程调用其他服务的接口方法，或者是远端服务具体的实现方法（接收传过来的 TransactionContext）
 */
public class ResourceCoordinatorInterceptor { // 主要是将参与者 Participant 加入到事务 Transaction 中

    private TransactionManager transactionManager;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }
    // 切入点是标注了 @Compensable 注解 || 参数列表中有 TransactionContext 类型的。比如 commit/rollback 方法参数里有 TransactionContext 也会被拦截
    public Object interceptTransactionContextMethod(TransactionMethodJoinPoint pjp) throws Throwable {

        Transaction transaction = transactionManager.getCurrentTransaction();
        // 上一个 CompensableTransactionInterceptor 拦截器创建的 Transaction != null && Try 阶段
        if (transaction != null && transaction.getStatus().equals(TransactionStatus.TRYING)) {
            // 添加事务参与者。标注了 @Compensable 注解的方法 || 参数列表中第一个参数是 TransactionContext 方法，其实就是远程调用其他服务的接口方法
            Participant participant = enlistParticipant(pjp);

            if (participant != null) {

                Object result = null;
                try { // 给事务参与者设置事务上下文，如果是远程调用其他服务的接口方法或者是远端服务具体的实现方法（接收传过来的 TransactionContext），会将 TransactionContext 传递到远端。比如 order 服务的 TradeOrderServiceProxy 在远程调用 Capital 服务的 CapitalTradeOrderService#record 方法的时候被将 TransactionContext 设置到参数里，远端调用的时候序列化传出去给远端接收
                    FactoryBuilder.factoryOf(participant.getTransactionContextEditorClass()).getInstance().set(new TransactionContext(transaction.getRootXid(), participant.getXid(), TransactionStatus.TRYING.getId(), ParticipantStatus.TRYING.getId()), pjp.getTarget(), pjp.getMethod(), pjp.getArgs());
                    result = pjp.proceed(pjp.getArgs());
                    participant.setStatus(ParticipantStatus.TRY_SUCCESS);
                } catch (Throwable e) {
                    participant.setStatus(ParticipantStatus.TRY_FAILED);

                    //if root transaction, here no need persistent transaction
                    // because following stage is rollback, transaction's status is changed to CANCELING and save
//                    transactionManager.update(participant);
//
                    throw e;
                }


                return result;
            }
        }
        // commit 和 rollback 阶段直接放行
        return pjp.proceed(pjp.getArgs());
    }
    // 添加事务参与者。标注了 @Compensable 注解的方法 || 参数列表中第一个参数是 TransactionContext 方法，其实就是远程调用其他服务的接口方法
    private Participant enlistParticipant(TransactionMethodJoinPoint pjp) {
        // 获取 当前线程事务第一个(头部)元素
        Transaction transaction = transactionManager.getCurrentTransaction();
        CompensableMethodContext compensableMethodContext = new CompensableMethodContext(pjp, transaction);
        // 获得 确认执行业务方法 和 取消执行业务方法
        String confirmMethodName = compensableMethodContext.getConfirmMethodName();
        String cancelMethodName = compensableMethodContext.getCancelMethodName();
        Class<? extends TransactionContextEditor> transactionContextEditorClass = compensableMethodContext.getTransactionContextEditorClass();
        // 创建参与者的 事务编号
        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());
        // 获得类
        Class targetClass = compensableMethodContext.getDeclaredClass();
        // 创建 确认执行方法调用上下文 和 取消执行方法调用上下文
        InvocationContext confirmInvocation = new InvocationContext(targetClass,
                confirmMethodName,
                compensableMethodContext.getMethod().getParameterTypes(), compensableMethodContext.getArgs());

        InvocationContext cancelInvocation = new InvocationContext(targetClass,
                cancelMethodName,
                compensableMethodContext.getMethod().getParameterTypes(), compensableMethodContext.getArgs());
        // 创建 事务参与者。拿发起方来说，标注了 @Compensable 注解的方法会创建第一个，远程调用其他服务的接口方法的时候会创建第二个、第三个...
        Participant participant = // 如果是被调用方，通常是标注了 @Compensable 注解的方法会创建第一个，因为再调用其他服务了，所以只有1个
                new Participant(
                        transaction.getRootXid(),
                        xid,
                        confirmInvocation,
                        cancelInvocation,
                        transactionContextEditorClass);
        // 添加 事务参与者 到 事务
        transactionManager.enlistParticipant(participant);
        return participant;
    }
}
