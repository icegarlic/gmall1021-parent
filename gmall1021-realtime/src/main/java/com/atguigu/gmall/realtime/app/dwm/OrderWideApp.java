package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * 处理订单和订单明细数据形成的订单宽表
 */
public class OrderWideApp {

    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/21 基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度读取kafka分区数据
//        env.setParallelism(4);
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint/OrderWideApp"));
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO: 2021/4/21 从kafka的dwd层接收订单和订单明细数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        DataStreamSource<String> orderInfoJsonStream = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailJsonStream = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        // 对数据及逆行结构转换
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = orderInfoJsonStream.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String jsonStr) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailStream = orderDetailJsonStream.map(new RichMapFunction<String, OrderDetail>() {
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String jsonStr) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });
//        orderInfoStream.print(">>>");
//        orderDetailStream.print("###");
        // TODO: 2021/4/21 加上水位线
        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTimeStream = orderInfoStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTimeStream = orderDetailStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        // TODO: 2021/4/21 设定关联key
        KeyedStream<OrderInfo, Long> orderInfoKeyedStream = orderInfoWithEventTimeStream.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithEventTimeStream.keyBy(OrderDetail::getOrder_id);

        // TODO: 2021/4/21 订单和订单明细关联 intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoKeyedStream.intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        orderWideStream.print("joined :: ");
        env.execute();
    }
}
