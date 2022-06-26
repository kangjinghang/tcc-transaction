package org.mengyun.tcctransaction.recovery;

import org.mengyun.tcctransaction.SystemException;
import org.quartz.*;

/**
 * Created by changming.xie on 6/2/16. 事务恢复定时任务，基于 Quartz 实现调度，不断执行事务恢复
 */
public class RecoverScheduledJob {
    // 事务恢复逻辑
    private TransactionRecovery transactionRecovery;

    private Scheduler scheduler;

    private String jobName;

    private String triggerName;

    private String cronExpression;

    private int delayStartSeconds;


    public void init() {

        try {
            JobDetail jobDetail = JobBuilder.newJob(QuartzRecoveryTask.class).withIdentity(jobName).build();
            jobDetail.getJobDataMap().put(QuartzRecoveryTask.RECOVERY_INSTANCE_KEY, transactionRecovery);

            CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity(triggerName)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)
                            .withMisfireHandlingInstructionDoNothing()).build();
            // 启动任务调度
            scheduler.scheduleJob(jobDetail, cronTrigger);

            scheduler.startDelayed(delayStartSeconds);

        } catch (Exception e) {
            throw new SystemException(e);
        }
    }

    public void setTransactionRecovery(TransactionRecovery transactionRecovery) {
        this.transactionRecovery = transactionRecovery;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public int getDelayStartSeconds() {
        return delayStartSeconds;
    }

    public void setDelayStartSeconds(int delayStartSeconds) {
        this.delayStartSeconds = delayStartSeconds;
    }
}
