package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.ProductStats;
import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.service.ProductStatsService;
import com.atguigu.gmall.service.ProvinceStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * 主要接受用户请求，并做出响应。根据sugar不同的组件，返回不同的格式
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;

    /*
    {
        "status": 0,
            "msg": "",
            "data": 1201081.1632389291
    }
     */
    // 数字牌
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";
        return json;

    }

    private int now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }

    /*
    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit){
        // 判断日期是否为空
        if (date == 0) {
            date = now();
        }
        // 调用service层方法，获取品牌及交易额
        List<ProductStats> productStatsList = productStatsService.getProductStatsByTrademark(date, limit);
        ArrayList<String> trademarkNameList = new ArrayList<>();
        ArrayList<BigDecimal> amountList = new ArrayList<BigDecimal>();

        // 对获取的list的集合进行遍历
        for (ProductStats productStats : productStatsList) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }
        String json = "{\"status\": 0,\"data\": " +
                "{\"categories\": [\""+ StringUtils.join(trademarkNameList,"\",\"")+"\"]," +
                "\"series\": [{\"name\": \"商品品牌\",\"data\": ["+ StringUtils.join(amountList,",") +"]}]}}";
        return json;
    }
     */
    /*
    {"status": 0,"msg": "",
            "data": {
        "categories": [
        "苹果",
                "三星"
    ],
        "series": [
        {
            "name": "手机品牌",
                "data": [
            8249,
                    7263
        ]}]}}
    */
    // 柱状图
    @RequestMapping("/trademark")
    public Object getProductStatsByTrademark(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        // 判断日期是否为空
        if (date == 0) {
            date = now();
        }
        // 调用service层方法，获取品牌及交易额
        List<ProductStats> productStatsList = productStatsService.getProductStatsByTrademark(date, limit);
        ArrayList<String> trademarkNameList = new ArrayList<>();
        ArrayList<BigDecimal> amountList = new ArrayList<BigDecimal>();

        // 对获取的list的集合进行遍历
        for (ProductStats productStats : productStatsList) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }

        Map resMap = new HashMap();
        resMap.put("status", 0);

        Map dataMap = new HashMap();
        dataMap.put("categories", trademarkNameList);


        Map seriesMap = new HashMap();
        seriesMap.put("name", "商品品牌");
        seriesMap.put("data", amountList);

        List seriesList = new ArrayList();
        seriesList.add(seriesMap);
        dataMap.put("series", seriesList);

        resMap.put("data", dataMap);
        return resMap;
    }

    /*
    {
        "status": 0,
        "data": [
            {
                "name": "数码类",
                "value": 371570
            },
            {
                "name": "日用品",
                "value": 296016
            }
        ]
    }
    */
    // 饼图
    // 方式1 拼接json字符串
    /*
    @RequestMapping("/category3")
    public Object getProductStatsByCategory3(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        if (date == 0) {
            date = now();
        }
        List<ProductStats> productStatsList = productStatsService.getProductStatsByCategory3(date, limit);

        StringBuilder jsonBuild = new StringBuilder("{\"status\": 0,\"data\": [");
        for (int i = 0; i < productStatsList.size(); i++) {
            ProductStats productStats = productStatsList.get(i);
            jsonBuild.append("{\"name\": \"" + productStats.getCategory3_name() + "\",\"value\": " + productStats.getOrder_amount() + "}");
            if (i < productStatsList.size() - 1) {
                jsonBuild.append(",");
            }
        }
        jsonBuild.append("]}");
        return jsonBuild.toString();
    }
    */
    @RequestMapping("/category3")
    public Object getProductStatsByCategory3(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit
    ) {
        if (date == 0) {
            date = now();
        }
        List<ProductStats> productStatsList = productStatsService.getProductStatsByCategory3(date, limit);

        Map resMap = new HashMap();
        resMap.put("status", 0);

        List dataList = new ArrayList();
        for (ProductStats productStats : productStatsList) {
            Map dataMap = new HashMap();
            dataMap.put("name", productStats.getCategory3_name());
            dataMap.put("value", productStats.getOrder_amount());
            dataList.add(dataMap);
        }
        resMap.put("data", dataList);

        return resMap;
    }

    @RequestMapping("/spu")
    public String getProductStatsBySpu(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit
    ) {
        if (date == 0) {
            date = now();
        }
        //调用service层方法，获取spu相关数据
        List<ProductStats> productStatsList = productStatsService.getProductStatsBySpu(date, limit);

        StringBuilder builder = new StringBuilder("{\"status\": 0,\"data\": " +
                "{\"columns\": [{\"name\": \"商品名称\",\"id\": \"spu_name\"}," +
                "{\"name\": \"交易额\",\"id\": \"order_amount\"}," +
                "{\"name\": \"订单数\",\"id\": \"order_ct\"}],\"rows\": [");

        for (int i = 0; i < productStatsList.size(); i++) {
            ProductStats productStats = productStatsList.get(i);
            if (i >= 1) {
                builder.append(",");
            }
            builder.append("{\"spu_name\": \"" + productStats.getSpu_name() + "\"," +
                    "\"order_amount\": \"" + productStats.getOrder_amount() + "\",\"order_ct\": \"" + productStats.getOrder_ct() + "\"}");

        }
        builder.append("]}}");
        return builder.toString();
    }

    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);
        StringBuilder builder = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceStatsList.size(); i++) {
            ProvinceStats provinceStats = provinceStatsList.get(i);
            builder.append("{\"name\": \""+provinceStats.getProvince_name()+"\",\"value\": "+provinceStats.getOrder_amount()+"}");
            if(i < provinceStatsList.size() - 1){
                builder.append(",");
            }
        }
        builder.append("],\"valueName\":\"交易额\"}}");
        return builder.toString();
    }
}
