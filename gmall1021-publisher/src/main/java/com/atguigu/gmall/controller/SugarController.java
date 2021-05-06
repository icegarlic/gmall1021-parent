package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.KeywordStats;
import com.atguigu.gmall.bean.ProductStats;
import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.bean.VisitorStats;
import com.atguigu.gmall.service.KeywordStatsService;
import com.atguigu.gmall.service.ProductStatsService;
import com.atguigu.gmall.service.ProvinceStatsService;
import com.atguigu.gmall.service.VisitorStatsService;
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

    @Autowired
    VisitorStatsService visitorStatsService;

    @Autowired
    KeywordStatsService keywordStatsService;

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
            builder.append("{\"name\": \"" + provinceStats.getProvince_name() + "\",\"value\": " + provinceStats.getOrder_amount() + "}");
            if (i < provinceStatsList.size() - 1) {
                builder.append(",");
            }
        }
        builder.append("],\"valueName\":\"交易额\"}}");
        return builder.toString();
    }

    @RequestMapping("/visitor")
    public String getVisitorsStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        List<VisitorStats> visitorStatsByNewFlagList = visitorStatsService.getVisitorStatsByNewFlag(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        // 循环将数据赋给新访客统计对象和老访客统计对象
        for (VisitorStats visitorStats : visitorStatsByNewFlagList) {
            if ("1".equals(visitorStats.getIs_new())) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }
        //把数据拼接入字符串
        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":" +
                "[{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新用户\",\"id\":\"new\"}," +
                "{\"name\":\"老用户\",\"id\":\"old\"}]," +
                "\"rows\":" +
                "[{\"type\":\"用户数(人)\"," +
                "\"new\": " + newVisitorStats.getUv_ct() + "," +
                "\"old\":" + oldVisitorStats.getUv_ct() + "}," +
                "{\"type\":\"总访问页面(次)\"," +
                "\"new\":" + newVisitorStats.getPv_ct() + "," +
                "\"old\":" + oldVisitorStats.getPv_ct() + "}," +
                "{\"type\":\"跳出率(%)\"," +
                "\"new\":" + newVisitorStats.getUjRate() + "," +
                "\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"平均在线时长(秒)\"," +
                "\"new\":" + newVisitorStats.getDurPerSv() + "," +
                "\"old\":" + oldVisitorStats.getDurPerSv() + "}," +
                "{\"type\":\"平均访问页面数(人次)\"," +
                "\"new\":" + newVisitorStats.getPvPerSv() + "," +
                "\"old\":" + oldVisitorStats.getPvPerSv()
                + "}]}}";
        return json;
    }

    @RequestMapping("/hr")
    public String getMidStatsGroupbyHourNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        List<VisitorStats> visitorStatsByHourList = visitorStatsService.getVisitorStatsByHour(date);

        // 构建24位数组
        VisitorStats[] visitorStatsArr = new VisitorStats[24];

        // 把对应小时的位置赋值
        for (VisitorStats visitorStats : visitorStatsByHourList) {
            visitorStatsArr[visitorStats.getHr()] = visitorStats;
        }

        List<String> hrList = new ArrayList<>();
        List<Long> uvList = new ArrayList<>();
        List<Long> pvList = new ArrayList<>();
        List<Long> newMidList = new ArrayList<>();

        // 循环出固定的0-23个小时 从结果map中查询对应的值
        for (int hr = 0; hr < 23; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats != null) {
                uvList.add(visitorStats.getUv_ct());
                pvList.add(visitorStats.getPv_ct());
                newMidList.add(visitorStats.getNew_uv());
            } else {
                //该小时没有流量补零
                uvList.add(0L);
                pvList.add(0L);
                newMidList.add(0L);
            }
            // 小时数不足俩位补零
            hrList.add(String.format("%02d", hr));
        }

        // 拼接字符串
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\"" + StringUtils.join(hrList, "\",\"") + "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":[" + StringUtils.join(uvList, ",") + "]}," +
                "{\"name\":\"pv\",\"data\":[" + StringUtils.join(pvList, ",") + "]}," +
                "{\"name\":\"新用户\",\"data\":[" + StringUtils.join(newMidList, ",") + "]}]}}";
        return json;

    }

    /*
    {
        "status": 0,
            "data": [
        {
            "name": "data",
                "value": 60679,
        },
        {
            "name": "dataZoom",
                "value": 24347,
        }
    ]
    }
     */
    @RequestMapping("/keyword")
    public Object getKeywordStats(@RequestParam(value = "date", defaultValue = "0") Integer date, @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        if (date == 0) {
            date = now();
        }

        List<KeywordStats> keywordStatsList = keywordStatsService.getKeywordStats(date, limit);

        Map resMap = new HashMap();
        resMap.put("status", 0);

        List dataList = new ArrayList();

        for (KeywordStats keywordStats : keywordStatsList) {
            Map dataMap = new HashMap();
            dataMap.put("name", keywordStats.getKeyword());
            dataMap.put("value", keywordStats.getCt());
            dataList.add(dataMap);
        }
        resMap.put("data", dataList);

        return resMap;
    }
}
