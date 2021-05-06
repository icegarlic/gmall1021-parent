package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.VisitorStats;

import java.util.List;

/**
 * 访问流量统计Service接口
 */
public interface VisitorStatsService {

    List<VisitorStats> getVisitorStatsByNewFlag(Integer date);

    List<VisitorStats> getVisitorStatsByHour(Integer date);

    Long getPv(Integer date);

    Long getUv(Integer date);
}
