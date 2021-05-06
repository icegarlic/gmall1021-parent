package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.KeywordStats;

import java.util.List;

/**
 * 关键词接口
 */
public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(Integer date, Integer limit);
}
