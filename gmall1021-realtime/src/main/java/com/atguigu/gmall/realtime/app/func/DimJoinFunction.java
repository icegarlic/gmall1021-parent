package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    /**
     * 获取维度关联的key值
     */
    String getKey(T obj);

    /**
     * 维度数据关联
     */
    void join(T obj, JSONObject dimInfoJsonObj) throws Exception;
}
