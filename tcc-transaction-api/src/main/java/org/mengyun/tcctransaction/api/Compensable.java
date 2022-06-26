package org.mengyun.tcctransaction.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by changmingxie on 10/25/15. 标记可补偿的方法注解
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Compensable {
    // 传播级别
    public Propagation propagation() default Propagation.REQUIRED;
    // 确认执行业务方法
    public String confirmMethod() default "";
    // 取消执行业务方法
    public String cancelMethod() default "";

    public boolean asyncConfirm() default false;

    public boolean asyncCancel() default false;
    // 事务上下文编辑
    public Class<? extends TransactionContextEditor> transactionContextEditor() default NullableTransactionContextEditor.class;
}