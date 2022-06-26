package org.mengyun.tcctransaction;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by changmingxie on 11/9/15. 【commit 和 rollback 这两个执行方法】的调用上下文，记录类、方法名、参数类型数组、参数数组。通过这些属性，可以执行提交 / 回滚事务。本质上，TCC 通过多个参与者的 try / confirm / cancel 方法，实现事务的最终一致性。
 */
public class InvocationContext implements Serializable {

    private static final long serialVersionUID = -7969140711432461165L;
    private Class targetClass; // 类
    // 方法名
    private String methodName;
    // 参数类型数组
    private Class[] parameterTypes;
    // 参数数组
    private Object[] args;

    private final Map<String, String> attachments = new ConcurrentHashMap<String, String>();

    public InvocationContext() {

    }

    public InvocationContext(Class targetClass, String methodName, Class[] parameterTypes, Object... args) {
        this.methodName = methodName;
        this.parameterTypes = parameterTypes;
        this.targetClass = targetClass;
        this.args = args;
    }

    public Object[] getArgs() {
        return args;
    }

    public Class getTargetClass() {
        return targetClass;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class[] getParameterTypes() {
        return parameterTypes;
    }

    public void addAttachment(String key, String value) {
        attachments.put(key, value);
    }
}
