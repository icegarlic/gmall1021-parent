package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品统计接口
 */
public interface ProductStatsService {
    /**
     * 获取某一天的交易总额
     */
    public BigDecimal getGMV(Integer date);

    /**
     * 获取某品牌一天的交易额
     */
    List<ProductStats> getProductStatsByTrademark(Integer date, Integer limit);

    /**
     * 获取某品类一天的交易额
     */
    List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit);

    /**
     * 获取某商品的一天的交易额
     */
    List<ProductStats> getProductStatsBySpu(Integer date, Integer limit);
}
