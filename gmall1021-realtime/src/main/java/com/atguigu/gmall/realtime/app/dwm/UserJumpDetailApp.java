package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.MyPatternProcessFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 用户跳出明细统计，使用flink CEP
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        // TODO: 2021/4/21 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint/UserJunmpDetailApp"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 30000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        // TODO: 2021/4/21 从kafka中读取数据
        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";
        // 读取kafka数据
        DataStreamSource<String> sourceStream = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        // 测试使用
//        DataStream<String> kafkaStream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":15000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":30000} "
//                );

        // TODO: 2021/4/21 对流中的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = sourceStream.map(JSON::parseObject);

        // TODO: 2021/4/21 指定WaterMark以及提取时间字段，并分组。
        SingleOutputStreamOperator<JSONObject> jsonObjStreamWithWaterMark = jsonObjStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        KeyedStream<JSONObject, String> keyedStream = jsonObjStreamWithWaterMark.keyBy(jsonOnj -> jsonOnj.getJSONObject("common").getString("mid"));

        // TODO: 2021/4/21 配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("first").where(
                        new SimpleCondition<JSONObject>() {
                            // 条件1：进入第一个页面
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                .next("second").where(
                        new SimpleCondition<JSONObject>() {
                            // 条件2：10s内有第二个页面
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                ).within(Time.seconds(10));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO: 2021/4/21 提取命中的数据(即超过10s未访问下一页面视为跳出)
        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag") {
        };
        SingleOutputStreamOperator<String> processStream = patternStream.process(new MyPatternProcessFunction(timeoutTag));

        // TODO: 2021/4/21 通过SideOutput 侧输出流超时数据
        DataStream<String> jumpStream = processStream.getSideOutput(timeoutTag);
        jumpStream.print("跳出>>");

        // TODO: 2021/4/21 将跳出数据写回到kafka的DWM层
        jumpStream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
