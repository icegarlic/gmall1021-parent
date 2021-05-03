package com.atguigu.gmall.service.ipml;

import com.atguigu.gmall.bean.ProductStats;
import com.atguigu.gmall.mapper.ProductStatsMapper;
import com.atguigu.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品统计实现接口
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return productStatsMapper.selectGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByTrademark(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCategory3(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySpu(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsBySpu(date, limit);
    }
}
