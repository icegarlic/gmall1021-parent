package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.ProductStats;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品统计Mapper
 */
public interface ProductStatsMapper {

    /**
     * 统计某日获取商品交易额
     */
    @Select("select sum(order_amount) order_amount from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGMV(Integer date);

    /**
     * 统计某日某品牌销售排行
     */
    @Select("select tm_id,tm_name,sum(order_amount) order_amount from product_stats_2021 where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByTrademark(Integer date, Integer limit);

    /**
     * 根据品类统计销售排行
     */
    @Select("select category3_name,category3_id,sum(order_amount) order_amount from product_stats_2021 where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name " +
            "having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByCategory3(Integer date, Integer limit);

    /**
     * 根据商品统计销售排行
     */
    @Select("select spu_id,spu_name,sum(order_amount) order_amount,sum(order_ct) order_ct " +
            " from product_stats_2021 where toYYYYMMDD(stt)=#{date} " +
            " group by spu_id,spu_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsBySpu(Integer date, Integer limit);

}

