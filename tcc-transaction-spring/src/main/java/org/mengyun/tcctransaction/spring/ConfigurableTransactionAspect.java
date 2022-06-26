package org.mengyun.tcctransaction.spring;

import org.aspectj.lang.annotation.Aspect;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.interceptor.CompensableTransactionAspect;
import org.mengyun.tcctransaction.interceptor.CompensableTransactionInterceptor;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;

import javax.annotation.PostConstruct;

/**
 * Created by changmingxie on 10/30/15. 可补偿事务拦截器切面实现类（第一个切面）
 */
@Aspect
public class ConfigurableTransactionAspect extends CompensableTransactionAspect implements Ordered {

    @Autowired
    private TransactionConfigurator transactionConfigurator;

    @PostConstruct
    public void init() {

        TransactionManager transactionManager = transactionConfigurator.getTransactionManager();

        CompensableTransactionInterceptor compensableTransactionInterceptor = new CompensableTransactionInterceptor();
        compensableTransactionInterceptor.setTransactionManager(transactionManager);

        this.setCompensableTransactionInterceptor(compensableTransactionInterceptor);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE; // 第一个切面
    }

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }
}
