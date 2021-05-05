package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 形成以商品为准的统计 曝光 点击 购物车 下单 支付 退单 评论数 宽表
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/28 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 设置检查点相关
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(5000);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        // TODO: 2021/4/28 从Kafka中获取数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewStream = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> orderWideStream = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideStream = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartInfoStream = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));
        DataStreamSource<String> favorInfoStream = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));
        DataStreamSource<String> refundInfoStream = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoStream = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));

        // TODO: 2021/4/28 对获取的数据流进行结构转换
        // 页面日志
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsStream = pageViewStream.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        // 将日志有json字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 获取pageJsonObj
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        // 获取页面id
                        String pageId = pageJsonObj.getString("page_id");
                        // 获取日志记录事件
                        Long ts = jsonObj.getLong("ts");
                        // 判断是否为页面商品的点击行为
                        if ("good_detail".equals(pageId)) {
                            // 获取商品的id
                            Long skuId = pageJsonObj.getLong("item");
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(skuId)
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }
                        // 获取商品的曝光行为
                        JSONArray displays = jsonObj.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject displaysJSONObj = displays.getJSONObject(i);
                                // 判断是否曝光的商品
                                if ("sku_id".equals(displaysJSONObj.getString("item_type"))) {
                                    Long skuId = displaysJSONObj.getLong("item");
                                    ProductStats productStats = ProductStats.builder()
                                            .sku_id(skuId)
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build();
                                    out.collect(productStats);
                                }
                            }
                        }

                    }
                }
        );
        // 收藏数据处理
        SingleOutputStreamOperator<ProductStats> favorInfoStatsStream = favorInfoStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long skuId = jsonObj.getLong("sku_id");
                        String createTime = jsonObj.getString("create_time");

                        return ProductStats.builder()
                                .sku_id(skuId)
                                .favor_ct(1L)
                                .ts(DateTimeUtil.toTs(createTime))
                                .build();
                    }
                }
        );
        // 加购数据的处理
        SingleOutputStreamOperator<ProductStats> cartInfoStatsStream = cartInfoStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long skuId = jsonObj.getLong("sku_id");
                        String createTime = jsonObj.getString("create_time");

                        return ProductStats.builder()
                                .sku_id(skuId)
                                .click_ct(1L)
                                .ts(DateTimeUtil.toTs(createTime))
                                .build();
                    }
                }
        );
        // 商品下单数据的处理
        SingleOutputStreamOperator<ProductStats> orderWideStatsStream = orderWideStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                        Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());
                        return ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                // 商品实付分摊之后的金额
                                .order_amount(orderWide.getSplit_total_amount())
                                // 注意：因为数据是订单明细和订单的宽表，有可能多条明细对应同一个订单
                                // 所以在统计商品下单数的时候，需要当到Set集合中去重
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .ts(ts)
                                .build();
                    }
                }
        );
        // 商品支付数据的处理
        SingleOutputStreamOperator<ProductStats> paymentStatsStream = paymentWideStream.map(
                jsonStr -> {
                    PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts)
                            .build();
                }
        );
        // 商品退款数据的处理
        SingleOutputStreamOperator<ProductStats> refundStatsStream = refundInfoStream.map(
                jsonStr -> {
                    JSONObject refundJsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                }
        );
        // 商品评论数据的处理
        SingleOutputStreamOperator<ProductStats> commonInfoStatsStream = commentInfoStream.map(
                jsonStr -> {
                    JSONObject commonJsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    return ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L)
                            .good_comment_ct(goodCt)
                            .ts(ts)
                            .build();
                }
        );

        // TODO: 2021/4/28 把统一的数据结构合并为一个流
        DataStream<ProductStats> unionStream = clickAndDisplayStatsStream.union(
                favorInfoStatsStream,
                cartInfoStatsStream,
                orderWideStatsStream,
                paymentStatsStream,
                refundStatsStream,
                commonInfoStatsStream
        );

        // TODO: 2021/4/28 设定事件时间与水位线
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermark = unionStream.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                        return productStats.getTs();
                    }
                }));
        // TODO: 2021/4/28 分组、开窗、聚合
        KeyedStream<ProductStats, Long> keyedStream = productStatsWithWatermark.keyBy(ProductStats::getSku_id);
        WindowedStream<ProductStats, Long, TimeWindow> windowedStream = keyedStream.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        SingleOutputStreamOperator<ProductStats> reduceStream = windowedStream.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));

                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct((long) stats1.getOrderIdSet().size());

                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct((long) stats1.getRefundOrderIdSet().size());
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct((long) stats1.getPaidOrderIdSet().size());

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getComment_ct());

                        return stats1;
                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        for (ProductStats productStats : elements) {
                            productStats.setStt(DateTimeUtil.toYMDhms(new Date(context.window().getStart())));
                            productStats.setEdt(DateTimeUtil.toYMDhms(new Date(context.window().getEnd())));
                            out.collect(productStats);

                        }
                    }
                }
        );
        // TODO: 2021/4/28 补充商品维度信息
        // 关联商品维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuStream = AsyncDataStream.unorderedWait(
                reduceStream,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS
        );
        // 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuStream = AsyncDataStream.unorderedWait(
                productStatsWithSkuStream,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );
        // 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Stream = AsyncDataStream.unorderedWait(
                productStatsWithSpuStream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );
        // 关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmStream = AsyncDataStream.unorderedWait(
                productStatsWithCategory3Stream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );

        productStatsWithTmStream.print(">>>>");

        // TODO: 2021/4/28 写入clickhouse
        productStatsWithTmStream.addSink(
                ClickHouseUtil.<ProductStats>getJdbcSink(
                        "insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO: 2021/5/5 将结果写入kafka
        productStatsWithTmStream
                .map(productStat -> JSON.toJSONString(productStat, new SerializeConfig(true)))
                .addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));

        env.execute();
    }
}
