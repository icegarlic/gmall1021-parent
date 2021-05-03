package com.atguigu.gmall.service.ipml;

import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.mapper.ProvinceStatsMapper;
import com.atguigu.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(Integer date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
