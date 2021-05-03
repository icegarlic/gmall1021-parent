package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/28 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        // 检查点相关设置
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint/"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 4000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        // TODO: 2021/4/29 定义流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // TODO: 2021/4/29 把数据源定义为动态表，加上水位线
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (" +
                "province_id BIGINT, " +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id STRING, " +
                "split_total_amount DOUBLE," +
                "create_time STRING," +
                "rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime - INTERVAL '3' SECOND)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        // TODO: 2021/4/29 分组、开窗、聚合
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                "province_id," +
                "province_name," +
                "province_area_code area_code," +
                "province_iso_code iso_code ," +
                "province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, " +
                "sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");


        // TODO: 2021/4/29 转换数据流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        provinceStatsDataStream.print(">>>");

        // TODO: 2021/4/29 写入clickhouse
        provinceStatsDataStream.addSink(ClickHouseUtil.
                <ProvinceStats>getJdbcSink("insert into  province_stats_2021  values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
