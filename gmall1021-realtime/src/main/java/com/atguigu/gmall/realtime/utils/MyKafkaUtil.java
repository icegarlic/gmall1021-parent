package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * 操作kafka工具类
 */
public class MyKafkaUtil {

    /**
     * kafka相关的配置
     */
    private final static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    /**
     * source Kafka消费者
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * sink kafka生产者
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 1000 * 15 + "");
//        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
        return new FlinkKafkaProducer<String>(topic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic, null, element.getBytes());
            }
        }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


    /**
     * 封装Kafka生产者 动态指定多个不同的主题
     */
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        // 如果超过15分钟没有更新状态，则超时。默认1分钟
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, serializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
