package org.mengyun.tcctransaction.recovery;

/**
 * Created by changming.xie on 6/1/16. 默认事务恢复配置实现
 */
public class DefaultRecoverFrequency implements RecoverFrequency {

    public static final RecoverFrequency INSTANCE = new DefaultRecoverFrequency();
    // 单个事务恢复最大重试次数为 30
    private int maxRetryCount = 30;
    // 单个事务恢复重试的间隔时间为 30 秒
    private int recoverDuration = 30; //30 seconds
    // 定时任务 cron 表达式，每 15 秒执行一次
    private String cronExpression = "0/15 * * * * ? ";
    // 按页查询，默认每页 500 个
    private int fetchPageSize = 500;

    private int concurrentRecoveryThreadCount = Runtime.getRuntime().availableProcessors() * 2;

    @Override
    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    @Override
    public int getRecoverDuration() {
        return recoverDuration;
    }

    public void setRecoverDuration(int recoverDuration) {
        this.recoverDuration = recoverDuration;
    }

    @Override
    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public int getConcurrentRecoveryThreadCount() {
        return concurrentRecoveryThreadCount;
    }

    public void setConcurrentRecoveryThreadCount(int concurrentRecoveryThreadCount) {
        this.concurrentRecoveryThreadCount = concurrentRecoveryThreadCount;
    }

    public int getFetchPageSize() {
        return fetchPageSize;
    }

    public void setFetchPageSize(int fetchPageSize) {
        this.fetchPageSize = fetchPageSize;
    }
}