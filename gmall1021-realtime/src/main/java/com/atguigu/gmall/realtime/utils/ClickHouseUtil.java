package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 操作CLickHouse的工具
 */
public class ClickHouseUtil {
    // 获取对ClickHouse的JdbcUtil
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        SinkFunction<T> sink = JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T t) throws SQLException {
                        Field[] fields = t.getClass().getDeclaredFields();
                        int skipOffset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            // 通过反射获得字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                // 如果存在该注解
                                System.out.println("跳过字段：" + field.getName());
                                skipOffset++;
                                continue;
                            }
                            field.setAccessible(true);
                            try {
                                Object fieldValue = field.get(t);
                                // i代表流对象字段的下标
                                // 公式：写入表字段位置下标 = 对象流对象字段下标 + 1 - 跳过字段的偏移量
                                ps.setObject(i + 1 - skipOffset, fieldValue);

                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new RuntimeException("CK写入失败");
                            }
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
        return sink;
    }
}
