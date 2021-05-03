package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 省市统计mapper接口
 */
public interface ProvinceStatsMapper {
    /**
     * 每个省市每天的销售额
     */
    @Select("select province_id,province_name,sum(order_amount) order_amount " +
            " from province_stats_2021 where toYYYYMMDD(stt)=#{date} " +
            " group by province_id,province_name")
    List<ProvinceStats> selectProvinceStats(Integer date);
}
