package com.atguigu.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkCDC_01_DS {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

// 需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置超时时间为1分钟
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //2.3 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000L));
//        //2.4 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021_realtime")
                ///可选配置项,如果不指定该参数,则会读取上一个配置中指定的数据库下的所有表的数据
                //注意：指定的时候需要使用"db.table"的方式
                .tableList("gmall2021_realtime.t_user")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();
    }

}
