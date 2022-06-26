package org.mengyun.tcctransaction.api;

import java.lang.reflect.Method;
// 默认事务上下文编辑器实现
public class ParameterTransactionContextEditor implements TransactionContextEditor {
    // 获得事务上下文在方法参数里的位置
    public static int getTransactionContextParamPosition(Class<?>[] parameterTypes) {

        int position = -1;

        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i].equals(org.mengyun.tcctransaction.api.TransactionContext.class)) {
                position = i;
                break;
            }
        }
        return position;
    }

    public static boolean hasTransactionContextParameter(Class<?>[] parameterTypes) {
        return getTransactionContextParamPosition(parameterTypes) >= 0;
    }

    public static TransactionContext getTransactionContextFromArgs(Object[] args) {

        TransactionContext transactionContext = null;

        for (Object arg : args) {
            if (arg != null && org.mengyun.tcctransaction.api.TransactionContext.class.isAssignableFrom(arg.getClass())) {

                transactionContext = (org.mengyun.tcctransaction.api.TransactionContext) arg;
            }
        }

        return transactionContext;
    }

    @Override
    public TransactionContext get(Object target, Method method, Object[] args) {
        int position = getTransactionContextParamPosition(method.getParameterTypes());

        if (position >= 0) {
            return (TransactionContext) args[position];
        } else {
            throw new RuntimeException("No TransactionContext parameter exist while get TransactionContext with ParameterTransactionContextEditor!");
        }
    }

    @Override
    public void set(TransactionContext transactionContext, Object target, Method method, Object[] args) {

        int position = getTransactionContextParamPosition(method.getParameterTypes());
        if (position >= 0) {
            args[position] = transactionContext; // 设置方法参数
        } else {
            throw new RuntimeException("No TransactionContext parameter exist while set TransactionContext with ParameterTransactionContextEditor!");
        }
    }
}
