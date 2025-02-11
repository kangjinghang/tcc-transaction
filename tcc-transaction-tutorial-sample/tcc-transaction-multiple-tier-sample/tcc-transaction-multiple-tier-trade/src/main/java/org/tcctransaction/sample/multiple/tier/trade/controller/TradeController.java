package org.tcctransaction.sample.multiple.tier.trade.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.tcctransaction.sample.multiple.tier.trade.service.TradeService;

@RestController
@RequestMapping("/")
public class TradeController {

    @Autowired
    TradeService tradeService;

    @RequestMapping(value = "place", method = {RequestMethod.POST, RequestMethod.GET})
    public void place() {
        tradeService.place();
    }
}
