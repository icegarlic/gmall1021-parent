package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 处理订单和订单明细数据形成的订单宽表
 * <p>
 * 用户维度关联测试
 * -需要启动进程以及应用
 * zk、kafka、hdfs、hbase、maxwell、redis、BaseDBApp、OrderWideApp
 * -前期准备
 * 维度历史数据的初始化
 * maxwell-bootstrap到指定库的表中进行全表查询，将查询出的数据交给maxwell服务处理
 * -执行流程
 * >模拟生成业务数据jar
 * >数据会生成到业务数据库MySQL中
 * >MySQL会将数据记录到Binlog中
 * >Maxwell会从Binlog中读取新增以及变化数据发送到Kafka的ods_base_db_m主题中
 * >BaseDBApp从ods_base_db_m主题中读取数据，对数据进行分流
 * *使用FlinkCDC读取配置表，并且使用广播状态向下广播
 * *结合配置表中的配置信息，对流中的数据进行处理
 * >事实数据，写回到Kafka的dwd层
 * >维度数据，保存到Hbase的表中
 * >OrderWideApp从dwd层读取订单和订单明细数据进行双流join
 * intervalJoin
 * >OrderWideApp将双流join之后的结果和维度表进行关联
 * *普通维度查询
 * *旁路缓存
 * *异步IO
 * &定义一个类，继承RichAsyncFunction
 * &重写asyncInvoke方法，在该方法中通过线程池获取线程，发送异步请求
 * &模板方法设计模式
 * & 接收异步请求的结果
 * resultFuture.complete(Collections.singleton(obj));
 * &使用异步维度关联
 * AsyncDataStream.unorderedWait(流,异步处理函数,超时时间,时间单位)
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

        // 对数据结构转换
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = orderInfoJsonStream.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String jsonStr) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailStream = orderDetailJsonStream.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String jsonStr) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });
//        orderInfoStream.print("订单>>");
//        orderDetailStream.print("订单明细>>");

        // TODO: 2021/4/21 加上水位线
        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTimeStream = orderInfoStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        })
        );
        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTimeStream = orderDetailStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        })
        );

        // TODO: 2021/4/21 设定关联key
        KeyedStream<OrderInfo, Long> orderInfoKeyedStream = orderInfoWithEventTimeStream.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithEventTimeStream.keyBy(OrderDetail::getOrder_id);

        // TODO: 2021/4/21 订单和订单明细关联 intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoKeyedStream
                .intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        orderWideStream.print("orderWide>>");

        // TODO: 2021/4/25 用户维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserStream = AsyncDataStream.unorderedWait(
                orderWideStream,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                        LocalDate birthdayLocalDate = LocalDate.parse(birthday);
                        LocalDate now = LocalDate.now();
                        Period period = Period.between(birthdayLocalDate, now);
                        int age = period.getYears();

                        orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                        orderWide.setUser_age(age);

                    }
                },
                60,
                TimeUnit.SECONDS
        );
        orderWideWithUserStream.print(">>");

        env.execute();
    }
}
