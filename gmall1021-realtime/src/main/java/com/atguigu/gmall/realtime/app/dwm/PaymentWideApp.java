package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 支付宽表处理的应用
 */
public class PaymentWideApp {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 设置并行度
        env.setParallelism(4);
        //1.2 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 30000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2.从kafka中读取数据
        String groupId = "payment_group";
        String orderWideTopic = "dwm_order_wide";
        String paymentTopic = "dwd_payment_info";
        //2.1 订单宽表数据(dwm_order_wide)
        DataStreamSource<String> orderSourceStream = env.addSource(MyKafkaUtil.getKafkaSource(orderWideTopic, groupId));
        //2.2 支付表数据(dwd_payment_info)
        DataStreamSource<String> paymentSourceStream = env.addSource(MyKafkaUtil.getKafkaSource(paymentTopic, groupId));

        //TODO 3.对数据进行结构的转换
        //3.1 订单宽表数据  jsonStr->OrderWide
        SingleOutputStreamOperator<OrderWide> orderWideJsonObjStream = orderSourceStream.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

        //3.2 支付表数据  jsonStr->PaymentInfo
        SingleOutputStreamOperator<PaymentInfo> paymentJsonObjStream = paymentSourceStream.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));

        //TODO 4.设置Watermark以及提取事件时间字段
        //4.1 订单宽表(创建时间ts)
        orderWideJsonObjStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                        String create_time = orderWide.getCreate_time();
                        return DateTimeUtil.toTs(create_time);
                    }
                })
        );
        //4.2 支付表(回调时间ts)

        //TODO 5.按照order_id对流中数据进行分组(指定双流join字段)
        //4.3 订单宽表(order_id)
        //4.3 支付表(order_id)

        //TODO 6.双流join(intervalJoin)  a.intervalJoin(b).between.process

        //TODO 7.将join之后的数据写回到kafka的dwm_payment_wide
    }
}
