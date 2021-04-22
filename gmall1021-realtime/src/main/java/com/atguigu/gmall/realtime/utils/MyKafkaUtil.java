package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 操作kafka工具类
 */
public class MyKafkaUtil {

    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    //source Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
    }

    // sink kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(kafkaServer, topic, new SimpleStringSchema());
    }

    // 封装Kafka生产者 动态指定多个不同的主题
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        // 如果15分钟没有更新状态，则超时 默认 1 分钟
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, serializationSchema, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
