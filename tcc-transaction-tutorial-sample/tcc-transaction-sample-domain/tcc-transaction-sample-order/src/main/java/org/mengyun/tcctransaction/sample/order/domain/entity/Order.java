package org.mengyun.tcctransaction.sample.order.domain.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Created by changming.xie on 3/25/16. 订单
 */
public class Order implements Serializable {

    private static final long serialVersionUID = -5908730245224893590L;
    private long id; // 订单编号
    // 支付( 下单 )用户编号
    private long payerUserId;
    // 收款( 商店拥有者 )用户编号
    private long payeeUserId;
    // 红包支付金额
    private BigDecimal redPacketPayAmount;
    // 账户余额支付金额
    private BigDecimal capitalPayAmount;
    // 订单状态 - DRAFT ：草稿  - CONFIRMED ：支付成功 - PAY_FAILED ：支付失败
    private String status = "DRAFT";
    // 商户订单号，使用 UUID 生成
    private String merchantOrderNo;

    private long version = 1l;
    // 订单明细数组。非存储字段
    private List<OrderLine> orderLines = new ArrayList<OrderLine>();

    public Order() {

    }

    public Order(long payerUserId, long payeeUserId) {
        this.payerUserId = payerUserId;
        this.payeeUserId = payeeUserId;
        this.merchantOrderNo = UUID.randomUUID().toString().replace("-", "");
    }

    public long getPayerUserId() {
        return payerUserId;
    }

    public long getPayeeUserId() {
        return payeeUserId;
    }

    public BigDecimal getTotalAmount() {

        BigDecimal totalAmount = BigDecimal.ZERO;

        for (OrderLine orderLine : orderLines) {

            totalAmount = totalAmount.add(orderLine.getTotalAmount());
        }
        return totalAmount;
    }

    public void addOrderLine(OrderLine orderLine) {
        this.orderLines.add(orderLine);
    }

    public List<OrderLine> getOrderLines() {
        return Collections.unmodifiableList(this.orderLines);
    }

    public void needToPay(BigDecimal redPacketPayAmount, BigDecimal capitalPayAmount) {
        this.redPacketPayAmount = redPacketPayAmount;
        this.capitalPayAmount = capitalPayAmount;
    }

    public BigDecimal getRedPacketPayAmount() {
        return redPacketPayAmount;
    }

    public BigDecimal getCapitalPayAmount() {
        return capitalPayAmount;
    }

    public String getMerchantOrderNo() {
        return merchantOrderNo;
    }

    public void setMerchantOrderNo(String merchantOrderNo) {
        this.merchantOrderNo = merchantOrderNo;
    }

    public long getId() {
        return id;
    }

    public String getStatus() {
        return status;
    }

    public void confirm() {
        this.status = "CONFIRMED";
    }

    public void cancelPayment() {
        this.status = "PAY_FAILED";
    }


    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        version++;
    }
}
