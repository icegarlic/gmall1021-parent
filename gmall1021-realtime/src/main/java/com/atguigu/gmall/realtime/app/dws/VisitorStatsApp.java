package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Desc: 访客主题统计
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
 * 进行关联  这是一个fulljoin
 * 可以考虑使用flinksql 完成
 * 维度：版本、渠道、地区、新老访客
 * 度量：pv、uv、ujc、druing_time、sv_c
 * 测试流程
 * -需要启动的进程
 * zk、kafka、logger.sh、hdfs
 * -需要启动的应用程序
 * BaseLogApp---分流
 * UniqueVisitApp---独立访客
 * UserJumpDetailApp----用户跳出明细
 * VisitorStatsApp ----访客主题统计
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/26 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        // TODO: 2021/4/26 从kafka读取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        DataStreamSource<String> pageViewStream = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitStream = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDetailStream = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

//        pageViewStream.print(">>>");
//        uniqueVisitStream.print("###");
//        userJumpDetailStream.print("@@@");

        // TODO: 2021/4/26 对读取数据进行结构转换 VisitorStats
        // 页面日志流
        SingleOutputStreamOperator<VisitorStats> pageLogStatsStream = pageViewStream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        jsonObj.getLong("ts")
                );
                // 判断当前日志记录是否存在上级页面
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    visitorStats.setSv_ct(1L);
                }
                return visitorStats;
            }
        });
        // 独立访客流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsStream = uniqueVisitStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );
        // 用户跳出流
        SingleOutputStreamOperator<VisitorStats> userJumpDetailStatsStream = uniqueVisitStream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        // TODO: 2021/4/26 将不同流的数据合并在一起形成一条流 union
        DataStream<VisitorStats> unionStream = pageLogStatsStream.union(
                uniqueVisitStatsStream,
                userJumpDetailStatsStream
        );

        // TODO: 2021/4/26 指定WaterMark以及提取事件事件字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkStream = unionStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                                return visitorStats.getTs();
                            }
                        })
        );

        // TODO: 2021/4/26 按照维度进行分组 Tuple4<版本、渠道、地区、新老访客>
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWatermarkStream.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(
                        visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new()
                );
            }
        });

        // TODO: 2021/4/26 开窗  window()
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // TODO: 2021/4/26 聚合 reduce()
        SingleOutputStreamOperator<VisitorStats> reduceStream = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats visitorStats1, VisitorStats visitorStats2) throws Exception {
                        visitorStats1.setPv_ct(visitorStats1.getPv_ct() + visitorStats2.getPv_ct());
                        visitorStats1.setSv_ct(visitorStats1.getSv_ct() + visitorStats2.getSv_ct());
                        visitorStats1.setDur_sum(visitorStats1.getDur_sum() + visitorStats2.getDur_sum());
                        visitorStats1.setUj_ct(visitorStats1.getUj_ct() + visitorStats2.getUj_ct());
                        visitorStats1.setUv_ct(visitorStats1.getUv_ct() + visitorStats2.getUv_ct());
                        return visitorStats1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        // 补全统计的起始和结束时间
                        for (VisitorStats visitorStats : elements) {
                            long start = context.window().getStart();
                            long end = context.window().getEnd();
                            visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                            visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                            // 向下游传递窗口中元素
                            out.collect(visitorStats);
                        }
                    }
                }
        );
        reduceStream.print("****");
        // TODO: 写入clickhouse
        reduceStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }

}
