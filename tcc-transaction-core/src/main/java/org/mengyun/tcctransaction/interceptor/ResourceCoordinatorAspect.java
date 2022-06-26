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
 * Created by changmingxie on 11/8/15. 资源协调者拦截器对应的切面。第二个切面。切入点是标注了 @Compensable 注解 || 参数列表中第一个参数是 TransactionContext 的
 */
@Aspect
public abstract class ResourceCoordinatorAspect {

    private ResourceCoordinatorInterceptor resourceCoordinatorInterceptor;
    // 切入点是标注了 @Compensable 注解 || 参数列表中第一个参数是 TransactionContext 的
    @Pointcut("@annotation(org.mengyun.tcctransaction.api.Compensable) || execution(* *(org.mengyun.tcctransaction.api.TransactionContext,..))")
    public void transactionResourcePointcut() {

    }


    @Around("transactionResourcePointcut()")
    public Object interceptTransactionResourceMethodWithCompensableAnnotation(ProceedingJoinPoint pjp) throws Throwable {

        Method method = ((MethodSignature) pjp.getSignature()).getMethod();

        Compensable compensable = method.getAnnotation(Compensable.class);

        Class<? extends TransactionContextEditor> transactionContextEditor = NullableTransactionContextEditor.class;

        if (compensable != null) {
            transactionContextEditor = compensable.transactionContextEditor();
        }
        // 切入点是标注了 @Compensable 注解 || 参数列表中第一个参数是 TransactionContext 的
        if (transactionContextEditor.equals(NullableTransactionContextEditor.class)
                && ParameterTransactionContextEditor.hasTransactionContextParameter(method.getParameterTypes())) {
            // 比如 order 服务的 TradeOrderServiceProxy 在远程调用 Capital 服务的 CapitalTradeOrderService#record 方法的时候被拦截，会进到这里，代理类会将 TransactionContext 传递给远端服务
            transactionContextEditor = ParameterTransactionContextEditor.class;
        }

        return interceptTransactionContextMethod(new AspectJTransactionMethodJoinPoint(pjp, compensable, transactionContextEditor));
    }

    public Object interceptTransactionContextMethod(TransactionMethodJoinPoint pjp) throws Throwable {
        return resourceCoordinatorInterceptor.interceptTransactionContextMethod(pjp);
    }

    public void setResourceCoordinatorInterceptor(ResourceCoordinatorInterceptor resourceCoordinatorInterceptor) {
        this.resourceCoordinatorInterceptor = resourceCoordinatorInterceptor;
    }

    public abstract int getOrder();
}
