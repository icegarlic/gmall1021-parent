package com.atguigu.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_02_SQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table user_info (" +
                "id INT," +
                " name STRING," +
                "age INT" +
                ") with (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password'= '123456'," +
                "'database-name' = 'gmall2021_realtime'," +
                "'table-name' = 't_user'" +
                ")");

        tableEnv.executeSql("select * from user_info").print();

        env.execute();
    }

}
