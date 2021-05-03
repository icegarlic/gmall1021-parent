package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 搜索关键字计算
 */
public class KeywordStatsSqlApp {
    public static void main(String[] args) {
        // TODO: 2021/4/30 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 4000));
//        System.setProperty("HADOOP_USER_NAME","atgiugu");

        // TODO: 2021/4/30 定义StreamTable流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // TODO: 2021/4/30 从Kafka读取数据，创建动态表
        String groupId = "keyword_stats_app";
        String pageLogSourceTopic = "dwd_page_log";
        tableEnv.executeSql("create table page_view (" +
                "common MAP<STRING,STRING>," +
                "page MAP<STRING,STRING>," +
                "ts BIGINT," +
                "rowtime AS TO");
        // TODO: 2021/4/30 对数据进行过滤，只保留搜索日志
        // TODO: 2021/4/30 使用自定义的分词函数，对搜索关键词进行分词
        // TODO: 2021/4/30 分组、开床、聚合
        // TODO: 2021/4/30 将表转换为流
        // TODO: 2021/4/30 将流中数据写到ck中

    }
}
