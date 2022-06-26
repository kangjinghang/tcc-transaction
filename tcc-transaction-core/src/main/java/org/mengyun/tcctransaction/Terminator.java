package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.StringUtils;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 10/30/15. 执行器
 */
public final class Terminator {

    public Terminator() {

    }
    // invocationContext就是每个参与者的 commit 和 rollback 方法上下文，Transaction 对通过这个方法实现对参与者的统一提交/回滚
    public static Object invoke(TransactionContext transactionContext, InvocationContext invocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {


        if (StringUtils.isNotEmpty(invocationContext.getMethodName())) {

            try {
                // 获得 参与者对象
                Object target = FactoryBuilder.factoryOf(invocationContext.getTargetClass()).getInstance();
                // 获得 方法
                Method method = null;

                method = target.getClass().getMethod(invocationContext.getMethodName(), invocationContext.getParameterTypes());
                // 设置 事务上下文TransactionContext 到正确的方法参数位置上
                FactoryBuilder.factoryOf(transactionContextEditorClass).getInstance().set(transactionContext, target, method, invocationContext.getArgs());
                // 执行方法
                return method.invoke(target, invocationContext.getArgs());

            } catch (Exception e) {
                throw new SystemException(e);
            }
        }
        return null;
    }
}
