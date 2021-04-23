package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * 计算UV(Unique Visitor)，计算独立访客
 * 其一，是识别出该访客打开的第一个页面，表示这个访客开始进入我们的应用
 * 其二，由于访客可以在一天中多次进入应用，所以我们要在一天的范围内进行去重
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint/UniqueVisitApp"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        // TODO: 2021/4/20 从kafka中读取数据
        String topic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";

        // 读取kafka指定的topic
        DataStreamSource<String> sourceStream = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));
        // 将字符串转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonObjStream = sourceStream.map(JSON::parseObject);
//        jsonObjStream.print("uv");

        // TODO: 2021/4/20 分组过滤
        // 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // 过滤
        SingleOutputStreamOperator<JSONObject> filterStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            // 定义状态变量存储最后访问日期
            ValueState<String> lastVisitDateState;
            // 定义日期格式转换器
            SimpleDateFormat sdf;

            /**
             * 初始化状态变量
             * 初始化日期格式
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sdf = new SimpleDateFormat("yyyyMMdd");
                if (lastVisitDateState == null) {
                    // 定义状态描述器
                    ValueStateDescriptor<String> lastVisitDateStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                    // 设置状态失效时间，1天
                    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                            // 默认设置，当状态创建和写入时都会更新状态戳
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            // 默认设置，当状态失效是不可访问
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();
                    // 状态描述器启用状态失效配置
                    lastVisitDateStateDescriptor.enableTimeToLive(ttlConfig);
                    lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDescriptor);
                }
            }

            /**
             * 检查当前页面是否有上页标识，如果有就说明该次不是当日首次访问
             */
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                // 获取当前也的上页Id
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                // 不为空过滤掉
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                // 如果没有上页标识，则获取ts和lastVisitDate
                Long ts = jsonObj.getLong("ts");
                String logDate = sdf.format(ts);
                String lastVisitDate = lastVisitDateState.value();

                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                    System.out.println("已访问：lastVisit:" + lastVisitDate + " || logDate：" + logDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisit:" + lastVisitDate + " || lagDate：" + logDate);
                    lastVisitDateState.update(logDate);
                    return true;
                }

            }
        }).uid("uvFilter");

        // TODO: 2021/4/20 结构转换并存入指定topic
        SingleOutputStreamOperator<String> mappedStream = filterStream.map(JSON::toString);
        mappedStream.print("uv：");
        mappedStream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
