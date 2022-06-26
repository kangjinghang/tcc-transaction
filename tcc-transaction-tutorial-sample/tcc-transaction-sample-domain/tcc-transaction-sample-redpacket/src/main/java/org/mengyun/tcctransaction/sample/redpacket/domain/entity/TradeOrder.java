package org.mengyun.tcctransaction.sample.redpacket.domain.entity;

import java.math.BigDecimal;
// 交易订单表
public class TradeOrder {
    // 交易订单编号
    private long id;
    // 转出用户编号
    private long selfUserId;
    // 转入用户编号
    private long oppositeUserId;
    // 商户订单号
    private String merchantOrderNo;
    // 金额
    private BigDecimal amount;
    // 交易订单状态。- DRAFT ：草稿 - CONFIRM ：交易成功 - CANCEL ：交易取消
    private String status = "DRAFT";

    private long version = 1l;

    public TradeOrder() {
    }

    public TradeOrder(long selfUserId, long oppositeUserId, String merchantOrderNo, BigDecimal amount) {
        this.selfUserId = selfUserId;
        this.oppositeUserId = oppositeUserId;
        this.merchantOrderNo = merchantOrderNo;
        this.amount = amount;
    }

    public long getId() {
        return id;
    }

    public long getSelfUserId() {
        return selfUserId;
    }

    public long getOppositeUserId() {
        return oppositeUserId;
    }

    public String getMerchantOrderNo() {
        return merchantOrderNo;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getStatus() {
        return status;
    }

    public void confirm() {
        this.status = "CONFIRM";
    }

    public void cancel() {
        this.status = "CANCEL";
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        version = version + 1;
    }
}
