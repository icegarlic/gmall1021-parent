package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema {

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

//        System.out.println(result.toJSONString());
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }
}
