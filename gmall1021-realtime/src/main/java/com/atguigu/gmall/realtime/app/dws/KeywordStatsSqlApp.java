package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 搜索关键字计算
 */
public class KeywordStatsSqlApp {
    public static void main(String[] args) throws Exception {
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
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND" +
                ") WITH(" + MyKafkaUtil.getKafkaDDL(pageLogSourceTopic, groupId) + ")");

        // TODO: 2021/4/30 对数据进行过滤，只保留搜索日志
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword,rowtime from page_view " +
                " where page['page_id'] = 'good_list' " +
                " and page['item'] IS NOT NULL");

        // TODO: 2021/4/30 使用自定义的分词函数，对搜索关键词进行分词
        Table keywordView = tableEnv.sqlQuery("select rowtime,keyword " +
                " from " + fullwordView + ", LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)");

        // TODO: 2021/4/30 分组、开床、聚合
        Table keywordStatsSearch = tableEnv.sqlQuery("select " +
                "keyword," +
                "count(*) ct,'"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from " + keywordView + "" +
                " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword");

        // TODO: 2021/4/30 将表转换为流
        DataStream<KeywordStats> keywordStatsStream = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);
        keywordStatsStream.print(">>>");

        // TODO: 2021/4/30 将流中数据写到ck中
        keywordStatsStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts)  " +
                        " values(?,?,?,?,?,?)"
                )
        );
        env.execute();

    }
}
