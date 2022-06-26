package org.mengyun.tcctransaction.sample.http.capital.api;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.sample.http.capital.api.dto.CapitalTradeOrderDto;

/**
 * Created by changming.xie on 4/1/16.
 */
public interface CapitalTradeOrderService {
    // 在CapitalTradeOrderService接口方法入参上加上TransactionContext 将提供的接口标记为tcc接口
    public String record(TransactionContext transactionContext, CapitalTradeOrderDto tradeOrderDto);
}
