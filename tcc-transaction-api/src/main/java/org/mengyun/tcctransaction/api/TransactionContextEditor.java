package org.mengyun.tcctransaction.api;

import java.lang.reflect.Method;

/**
 * Created by changming.xie on 1/18/17. 事务上下文编辑器，，用于设置和获得事务上下文
 */
public interface TransactionContextEditor {
    // 从参数中获得事务上下文
    public TransactionContext get(Object target, Method method, Object[] args);
    // 设置事务上下文到参数中
    public void set(TransactionContext transactionContext, Object target, Method method, Object[] args);

}
