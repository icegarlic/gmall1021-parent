package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO: 2021/4/16 3.将字符串转换为Json对象
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> streamSource = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjStream = streamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        // TODO: 2021/4/16 4.状态进行修复
        SingleOutputStreamOperator<JSONObject> mappedStream = jsonObjStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    ValueState<String> firstVisitState;
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitState", Types.STRING));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        // 获取状态标记
                        String isNew = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            // 获取当前状态，第一次访问日期
                            String firstVisitDate = firstVisitState.value();
                            // 当前访问日期
                            String curDate = sdf.format(value.getLong("ts"));
                            if (firstVisitDate != null && firstVisitDate.length() != 0) {
                                // 设备已经访问过
                                if (!firstVisitDate.equals(curDate)) {
                                    isNew = "0";
                                    value.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                // 设备第一次访问
                                firstVisitState.update(curDate);
                            }
                        }
                        return value;
                    }
                });

        // TODO: 2021/4/16 5.利用侧输出流完成分流操作   启动日志->启动侧输出流 曝光日志->曝光侧输出流  页面入职->主流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> splitStream = mappedStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        JSONObject startJsonObj = value.getJSONObject("start");
                        String jsonString = value.toJSONString();
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            // 启动日志侧流
                            ctx.output(startTag, jsonString);
                        } else {
                            out.collect(jsonString);
                            // 判断曝光日志侧流
                            JSONArray displaysArr = value.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                String pageId = value.getJSONObject("page").getString("page_id");
                                // 给每个曝光类型加上pageId
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                                    displayJsonObj.put("page_id", pageId);
                                    ctx.output(displayTag, displayJsonObj.toJSONString());
                                }
                            }
                        }
                    }
                }
        );

        DataStream<String> startDS = splitStream.getSideOutput(startTag);
        startDS.print("启动");
        DataStream<String> displayDS = splitStream.getSideOutput(displayTag);
        displayDS.print("曝光");
        splitStream.print("页面");

        // TODO: 2021/4/17 6.将不同的流的数据写回到Kafka的dwd层
        // 6.1 声明相关主题
        String startTopic = "dwd_start_log";
        String displayTopic = "dwd_display_log";
        String pageTopic = "dwd_page_log";
        startDS.addSink(MyKafkaUtil.getKafkaSink(startTopic));
        displayDS.addSink(MyKafkaUtil.getKafkaSink(displayTopic));
        splitStream.addSink(MyKafkaUtil.getKafkaSink(pageTopic));


        env.execute();
    }

}
