package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 处理超过10s未访问页面的数据
 */
public class MyPatternProcessFunction extends PatternProcessFunction<JSONObject, String> implements TimedOutPartialMatchHandler<JSONObject> {

    OutputTag<String> timeoutTag;

    public MyPatternProcessFunction(OutputTag<String> timeoutTag) {
        this.timeoutTag = timeoutTag;
    }

    @Override
    public void processMatch(Map<String, List<JSONObject>> match, Context ctx, Collector<String> out) throws Exception {

    }

    @Override
    public void processTimedOutMatch(Map<String, List<JSONObject>> match, Context ctx) throws Exception {
        JSONObject jsonObject = match.get("first").get(0);
        ctx.output(timeoutTag, jsonObject.toJSONString());
    }
}
