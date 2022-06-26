package org.mengyun.tcctransaction.recovery;


/**
 * Created by changming.xie on 6/1/16. 事务恢复配置接口
 */
public interface RecoverFrequency {
    // 单个事务恢复最大重试次数。超过最大重试次数后，目前仅打出错误日志
    int getMaxRetryCount();

    int getFetchPageSize();
    // 单个事务恢复重试的间隔时间，单位：秒。
    int getRecoverDuration();
    // 定时任务 cron 表达式
    String getCronExpression();

    int getConcurrentRecoveryThreadCount();
}
