package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 从Phoenix中查询维度数据的工具类
 */
public class DimUtil {
    //直接从Phoenix中查询维度数据，没有缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //定义维度查询的SQL
        String dimSql = "select * from " + tableName + " where ";
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            if (i > 0) {
                dimSql += " and ";
            }
            dimSql += columnName + " ='" + columnValue + "' ";
        }

        System.out.println("维度查询的SQL:" + dimSql);

        JSONObject dimInfoJsonObj = null;
        //调用封装的PhoenixUtil工具类中的查询方法
        List<JSONObject> dimInfoList = PhoenixUtil.queryList(dimSql, JSONObject.class);
        if (dimInfoList != null && dimInfoList.size() > 0) {
            dimInfoJsonObj = dimInfoList.get(0);
        } else {
            System.out.println("没有查询到维度数据:" + dimSql);
        }
        return dimInfoJsonObj;
    }


    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName,Tuple2.of("id",id));
    }
    /*
        旁路缓存:先从缓存Redis中查询维度数据，如果Redis中存在，那么直接返回；
            如果Redis中不存在，再到Phoenix中查询，并且将查询到的维度数据放到Redis中缓存起来
        Redis
            type:   String
            key:    dim:维度表名:14_cc
            value:  json格式字符串
            ttl:    1day
    */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //定义维度查询的SQL
        String dimSql = "select * from " + tableName + " where ";
        //定义操作Redis的key     dim:dim_base_trademark:
        String redisKey = "dim:" + tableName.toLowerCase() + ":";

        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            if (i > 0) {
                dimSql += " and ";
                redisKey += "_";
            }
            dimSql += columnName + " ='" + columnValue + "' ";
            //dim:dim_base_trademark:15_qq
            redisKey += columnValue;
        }

        //根据redisKey到redis中查询维度数据
        Jedis jedis = null;
        String dimJsonStr = null;
        JSONObject dimInfoJsonObj = null;

        try {
            jedis = RedisUtil.getJedis();
            dimJsonStr = jedis.get(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Redis缓存中获取维度数据失败:" + redisKey);
        }

        //判断是否从Redis缓存中获取到了数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            dimInfoJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //在缓存中没有查到维度数据
            System.out.println("维度查询的SQL:" + dimSql);
            //调用封装的PhoenixUtil工具类中的查询方法
            List<JSONObject> dimInfoList = PhoenixUtil.queryList(dimSql, JSONObject.class);
            if (dimInfoList != null && dimInfoList.size() > 0) {
                dimInfoJsonObj = dimInfoList.get(0);
                //将查询的结果写到Redis中
                if (jedis != null) {
                    jedis.setex(redisKey,3600*24,dimInfoJsonObj.toJSONString());
                }
            } else {
                System.out.println("没有查询到维度数据:" + dimSql);
            }
        }

        //关闭Redis连接
        if(jedis != null){
            jedis.close();
        }
        return dimInfoJsonObj;
    }

    //失效Redis中的缓存
    public static void deleteCached(String tableName,String id){
        //定义redisKey
        String redisKey = "dim:" + tableName.toLowerCase() + ":"+id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("删除Redis中的缓存失败");
        }
    }

    public static void main(String[] args) {
        //JSONObject dimInfo = getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "15"));
        JSONObject dimInfo = getDimInfo("DIM_BASE_TRADEMARK", "12");

        System.out.println(dimInfo);
    }
}

