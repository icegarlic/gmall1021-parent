package com.atguigu.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

// 自定义序列化方式
public class FlinkCDC_03_CustomSchema {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchemaFunction())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        mysqlDS.print();
        env.execute();
    }

    public static class MySchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//            String topic = sourceRecord.topic();
//            String[] topicArr = topic.split("\\.");
//            String dbName = topicArr[1];
//            String tableName = topicArr[2];

            // 获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //获取变更的数据  value=Struct{after=Struct{id=3,name=ww11,age=55666}
            Struct valueStruct = (Struct) sourceRecord.value();
            String dbName = valueStruct.getStruct("source").getString("db");
            String tableName = valueStruct.getStruct("source").getString("table");
            Struct afterStruct = valueStruct.getStruct("after");
//            System.out.println(dbName);
//            System.out.println(tableName);
            // 将变更数据封装为一个json对象
            JSONObject dataJsonObj = new JSONObject();

            if (afterStruct != null) {
                for (Field fied : afterStruct.schema().fields()) {
                    String o = afterStruct.getString(fied.name());
//                    System.out.println(fied.name()+":"+o);
                    dataJsonObj.put(fied.name(), o);
                }
            }
            // 创建JSON对象用于封装最终返回值数据信息
            JSONObject result = new JSONObject();
            result.put("operation", operation.toString().toLowerCase());
            result.put("data",dataJsonObj);
            result.put("database", dbName);
            result.put("table",tableName);

            System.out.println(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    public static class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
            // 定义JSON对象用于存放反序列化后的数据
            JSONObject result = new JSONObject();

            // 获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            Struct valueStruct = (Struct) sourceRecord.value();
            // 获取库名，表名
            Struct sourceStruct = valueStruct.getStruct("source");
            String dbName = sourceStruct.getString("db");
            String tableName = sourceStruct.getString("table");
            // 获取修改的数据
            Struct after = valueStruct.getStruct("after");
            JSONObject value = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    String name = field.name();
                    String datas = after.getString(name);
                    value.put(name, datas);
                }
            }
            // 将完整数据放入JSON对象
            result.put("database", dbName);
            result.put("table", tableName);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }
            result.put("type", type);
            result.put("data", value);

//            System.out.println(result.toJSONString());
        collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

}
