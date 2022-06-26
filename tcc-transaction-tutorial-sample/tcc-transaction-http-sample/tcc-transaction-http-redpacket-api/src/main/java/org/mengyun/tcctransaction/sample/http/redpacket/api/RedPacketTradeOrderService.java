package org.mengyun.tcctransaction.sample.http.redpacket.api;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.sample.http.redpacket.api.dto.RedPacketTradeOrderDto;

/**
 * Created by changming.xie on 4/1/16.
 */
public interface RedPacketTradeOrderService {
    // 接口方法入参上加上TransactionContext 将提供的接口标记为tcc接口
    public String record(TransactionContext transactionContext, RedPacketTradeOrderDto tradeOrderDto);
}
