package org.mengyun.tcctransaction.sample.dubbo.redpacket.api;

import org.mengyun.tcctransaction.api.EnableTcc;
import org.mengyun.tcctransaction.sample.dubbo.redpacket.api.dto.RedPacketTradeOrderDto;

/**
 * Created by changming.xie on 4/1/16.
 */
public interface RedPacketTradeOrderService {
    // 声明tcc接口。在接口方法上加上@EnableTcc(1.7.x新增注解，1.6.x是@Compensable）将提供的接口标记为tcc接口:
    @EnableTcc
    public String record(RedPacketTradeOrderDto tradeOrderDto);
}
