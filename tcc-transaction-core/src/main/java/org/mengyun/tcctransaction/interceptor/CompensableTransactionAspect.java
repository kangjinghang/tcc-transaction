package org.mengyun.tcctransaction.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.NullableTransactionContextEditor;
import org.mengyun.tcctransaction.api.ParameterTransactionContextEditor;
import org.mengyun.tcctransaction.api.TransactionContextEditor;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 10/30/15. 可补偿事务拦截器切面抽象类（第一个切面），切的是 @Compensable 注解标注的，通常是 发起和参与 分布式事务的各个服务标注了 @Compensable 的 Try 方法
 */
@Aspect
public abstract class CompensableTransactionAspect {

    private CompensableTransactionInterceptor compensableTransactionInterceptor;

    public void setCompensableTransactionInterceptor(CompensableTransactionInterceptor compensableTransactionInterceptor) {
        this.compensableTransactionInterceptor = compensableTransactionInterceptor;
    }

    @Pointcut("@annotation(org.mengyun.tcctransaction.api.Compensable)")
    public void compensableTransactionPointcut() {

    }

    @Around("compensableTransactionPointcut()")
    public Object interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {

        Method method = ((MethodSignature) pjp.getSignature()).getMethod();

        Compensable compensable = method.getAnnotation(Compensable.class);

        Class<? extends TransactionContextEditor> transactionContextEditor = NullableTransactionContextEditor.class;

        if (compensable != null) { // dubbo TransactionContext 隐式传递，对业务代码侵入性较小
            transactionContextEditor = compensable.transactionContextEditor();
        }
        // http?如果参数列表里有 TransactionContext 类型的参数，transactionContextEditor 要设置成 ParameterTransactionContextEditor 类型的，就是说方法里要用到 TransactionContext，也可能要被 http 接口传递到远端
        if (transactionContextEditor.equals(NullableTransactionContextEditor.class)
                && ParameterTransactionContextEditor.hasTransactionContextParameter(method.getParameterTypes())) {

            transactionContextEditor = ParameterTransactionContextEditor.class;
        }

        return compensableTransactionInterceptor.interceptCompensableMethod(new AspectJTransactionMethodJoinPoint(pjp, compensable, transactionContextEditor));
    }

    public abstract int getOrder();
}
