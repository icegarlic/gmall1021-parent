package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.ProvinceStats;

import java.util.List;

public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(Integer date);
}
