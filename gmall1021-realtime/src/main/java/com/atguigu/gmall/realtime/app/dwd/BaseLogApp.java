package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * 从Kafka的ODS层读取日志数据，并根据不同的日志类型（启动、曝光，页面）进行分流
 * 将分流后的数据写回kafka
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/16 1.创建基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO: 2021/4/16 2.从kafka ods主题获取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        // TODO: 2021/4/16 3.将字符串转换为Json对象
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaSource.map(JSON::parseObject);

        // TODO: 2021/4/16 4.状态进行修复
        // 先分流在进行状态修复
        SingleOutputStreamOperator<JSONObject> mappedStream = jsonObjStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    ValueState<String> firstVisitState;
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitState", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        // 获取标记状态
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            // 当前状态中的访问日期
                            String firstVisitDate = firstVisitState.value();
                            // 当前访问日期
                            String curDate = sdf.format(jsonObj.getLong("ts"));
                            if (firstVisitDate != null && firstVisitDate.length() > 0) {
                                // 该设备已经访问过
                                if (!firstVisitDate.equals(curDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                // 该设备第一次访问，将访问时间存入状态
                                firstVisitState.update(curDate);
                            }
                        }
                        return jsonObj;
                    }
                });
//        mappedStream.print("修复后数据>>");

        // TODO: 2021/4/16 5.利用侧输出流完成分流操作   启动日志->启动侧输出流 曝光日志->曝光侧输出流  页面入职->主流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> splitStream = mappedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                // 将Json对象转换未Json字符串
                String jsonStr = jsonObj.toJSONString();

                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                if (startJsonObj != null && jsonObj.size() > 0) {
                    // 启动日志侧流
                    ctx.output(startOutputTag, jsonStr);
                } else {
                    // 其余在主流
                    out.collect(jsonStr);
                    // 判断是否需要输出到 曝光流
                    JSONArray displaysArr = jsonObj.getJSONArray("displays");
                    if (displaysArr != null && displaysArr.size() > 0) {
                        // 给每个曝光类型加上页面Id
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displaysArr.size(); i++) {
                            JSONObject displaysJsonObj = displaysArr.getJSONObject(i);
                            displaysJsonObj.put("page_id", pageId);
                            ctx.output(displayOutputTag, displaysJsonObj.toJSONString());
                        }
                    }
                }
            }
        });
        DataStream<String> startStream = splitStream.getSideOutput(startOutputTag);
        startStream.print("start");
        DataStream<String> displayStream = splitStream.getSideOutput(displayOutputTag);
        displayStream.print("display");
        splitStream.print("split");
        // TODO: 2021/4/17 6.将不同的流的数据写回到Kafka的dwd层
        String startTopic = "dwd_start_log";
        String displayTopic = "dwd_display_log";
        String pageTopic ="dwd_page_log";
        startStream.addSink(MyKafkaUtil.getKafkaSink(startTopic));
        displayStream.addSink(MyKafkaUtil.getKafkaSink(displayTopic));
        splitStream.addSink(MyKafkaUtil.getKafkaSink(pageTopic));

        env.execute();
    }

}
