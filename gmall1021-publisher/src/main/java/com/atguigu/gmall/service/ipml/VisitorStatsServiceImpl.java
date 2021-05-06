package com.atguigu.gmall.service.ipml;

import com.atguigu.gmall.bean.VisitorStats;
import com.atguigu.gmall.mapper.VisitorStatsMapper;
import com.atguigu.gmall.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 访问流量统计Service实现类
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }

    @Override
    public Long getPv(Integer date) {
        return visitorStatsMapper.selectPv(date);
    }

    @Override
    public Long getUv(Integer date) {
        return visitorStatsMapper.selectUv(date);
    }
}
