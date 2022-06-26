package org.mengyun.tcctransaction.spring;

import org.aspectj.lang.annotation.Aspect;
import org.mengyun.tcctransaction.interceptor.ResourceCoordinatorAspect;
import org.mengyun.tcctransaction.interceptor.ResourceCoordinatorInterceptor;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;

import javax.annotation.PostConstruct;

/**
 * Created by changmingxie on 11/8/15. 资源协调者拦截器对应的切面实现类。第二个切面
 */
@Aspect
public class ConfigurableCoordinatorAspect extends ResourceCoordinatorAspect implements Ordered {


    @Autowired
    private TransactionConfigurator transactionConfigurator;

    @PostConstruct
    public void init() {

        ResourceCoordinatorInterceptor resourceCoordinatorInterceptor = new ResourceCoordinatorInterceptor();
        resourceCoordinatorInterceptor.setTransactionManager(transactionConfigurator.getTransactionManager());
        this.setResourceCoordinatorInterceptor(resourceCoordinatorInterceptor); // 设置资源协调者拦截器
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1; // 第二个切面
    }

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }
}
