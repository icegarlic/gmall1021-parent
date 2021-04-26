package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.MyDeserializationSchemaFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * 业务数据的动态分流并 写入对应存储空间,维度 -> hbase,事实 -> kafka
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/17 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
          // job 失败保存检查点
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO: 2021/4/17 2.从kafka中读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        // TODO: 2021/4/17 对流中的数据进行结构转换 string -> JsonOBj
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaSource.map(JSON::parseObject);

        // TODO: 2021/4/17 简单的ETL
        SingleOutputStreamOperator<JSONObject> filteredStream = jsonObjStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                boolean flag = jsonObj.getString("table") != null
                        && jsonObj.getString("table").length() > 0
                        && jsonObj.getJSONObject("data") != null
                        && jsonObj.getString("data").length() > 3;
                return flag;

            }
        });

        // TODO: 2021/4/18 使用flinkCDC读取配置表形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021_realtime") // monitor all tables under inventory database
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchemaFunction()) // converts SourceRecord to String
                .build();
        DataStreamSource<String> CDCStream = env.addSource(sourceFunction);
//        CDCStream.print("cdc>>>");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor", Types.STRING, Types.POJO(TableProcess.class));
        BroadcastStream<String> broadcastStream = CDCStream.broadcast(mapStateDescriptor);

        // TODO: 2021/4/18 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filteredStream.connect(broadcastStream);
        // TODO: 2021/4/18 对数据进行分流操作 维度数据->侧输出流  事实数据放->放到主流
        OutputTag<JSONObject> dimOutputTag = new OutputTag<JSONObject>("dimOutputTag") {
        };
        SingleOutputStreamOperator<JSONObject> splitStream = connectedStream.process(new TableProcessFunction(dimOutputTag, mapStateDescriptor));
        DataStream<JSONObject> dimOutputStream = splitStream.getSideOutput(dimOutputTag);
        dimOutputStream.print("维度》》");
        splitStream.print("事实》》");

        // TODO: 2021/4/19 将维度侧输出流的数据插入到Phoenix中
        dimOutputStream.addSink(new DimSink());
        splitStream.addSink(MyKafkaUtil.<JSONObject>getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long aLong) {
                // 获取保存到Kafka的哪个主题中
                String topicName = jsonObj.getString("sink_table");
                // 获取data数据
                JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                return new ProducerRecord<>(topicName, dataJsonObj.toString().getBytes());
            }
        }));
        env.execute();
    }
}
