package com.atguigu.gmall.service.ipml;

import com.atguigu.gmall.bean.KeywordStats;
import com.atguigu.gmall.mapper.KeywordStatsMapper;
import com.atguigu.gmall.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(Integer date, Integer limit) {
        return keywordStatsMapper.selectKeywordStats(date, limit);
    }
}
