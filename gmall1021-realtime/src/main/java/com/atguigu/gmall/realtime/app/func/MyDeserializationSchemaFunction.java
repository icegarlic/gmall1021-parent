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

/**
 * Flink CDC 自定义反序列化器
 */
public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        // 定义JSON对象用来存放反序列化后的数据
        JSONObject result = new JSONObject();

//        SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={file=mysql-bin.000121, pos=154, row=1, snapshot=true}} ConnectRecord{topic='mysql_binlog_source.gmall2021_realtime.t_user', kafkaPartition=null, key=null, keySchema=null, value=Struct{after=Struct{name=lisi},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=gmall2021_realtime,table=t_user,server_id=0,file=mysql-bin.000121,pos=154,row=0},op=c,ts_ms=1619081966452}, valueSchema=Schema{mysql_binlog_source.gmall2021_realtime.t_user.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        // 获取库名，表名
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");
        String dbName = sourceStruct.getString("db");
        String tableName = sourceStruct.getString("table");
        // 获取修改后的数据
        Struct after = valueStruct.getStruct("after");
        // 定义一个JSON对象用来存放 data数据
        JSONObject dataObj = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                String name = field.name();
                String value = after.getString(name);
                dataObj.put(name, value);
            }
        }
        // 将完整数据放入最后结果
        result.put("database",dbName);
        result.put("table",tableName);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        result.put("type",type);
        result.put("data",dataObj);

//        System.out.println(result.toJSONString());
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }
}