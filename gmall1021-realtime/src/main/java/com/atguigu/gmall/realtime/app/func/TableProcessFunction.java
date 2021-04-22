package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 分流处理函数
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    /**
     * 声明数据库连接
     */
    private Connection conn;

    /**
     * 分流标记
     */
    private OutputTag<JSONObject> dimOutputTag;
    /**
     * 状态描述器
     */
    private final MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> dimOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimOutputTag = dimOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 处理主流中数据
     */
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 从状态获取配置信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取业务表名
        String tableName = jsonObj.getString("table");
        // 获取操作类型
        String type = jsonObj.getString("type");

        // 注：如果使用maxwell的bootstrap接受历史数据，接收的类型bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObj.put("type", type);
        }

        // 拼接状态中的 key
        String key = tableName + ":" + type;
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            // 获取数据信息
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            // 获取输出的地的表名或者主题名
            jsonObj.put("sink_table", tableProcess.getSinkTable());

            // 根据配置表中的sink_column对数据进行字段过滤
            String sinkColumns = tableProcess.getSinkColumns();
            if (sinkColumns != null && sinkColumns.length() > 0) {
                filterColumn(dataJsonObj, sinkColumns);
            }

            // 分流、判断是事实表还是维度表
            if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                // 维度表--放到维度侧输出流
                ctx.output(dimOutputTag, jsonObj);
            } else if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                // 事实表，放到主流
                out.collect(jsonObj);
            }
        } else {
            System.out.println("No this Key in TableProcess: " + key);
        }

    }

    //根据配置表中的sinkColumn配置，对json对象中的数据进行过滤，将没有在配置表中出现的字段过滤掉
    //流中数据：{"id":XX,"tm_name":"","log_url"}  ------------配置表中sinkColumn  id,tm_name
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(ele->!fieldList.contains(ele.getKey()));
    }

    /**
     * 处理广播流中的数据配置信息 jsonStr: {"database":"","table":"","type":"","data":{}}
     */
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        // 将字符串转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String dataJsonStr = jsonObj.getString("data");
        // 将data字符串转换为TableProcess实体类对象
        TableProcess tableProcess = JSON.parseObject(dataJsonStr, TableProcess.class);
        if (tableProcess != null) {
            // 获取业务数据库表明
            String sourceTable = tableProcess.getSourceTable();
            // 获取操作类型
            String operateType = tableProcess.getOperateType();
            // 用于标记是维度还是事实
            String sinkType = tableProcess.getSinkType();
            // 获取输出到目的地的表名或者主题名
            String sinkTable = tableProcess.getSinkTable();
            // 获取输出的列
            String sinkColumns = tableProcess.getSinkColumns();
            // 获取主键
            String sinkPk = tableProcess.getSinkPk();
            // 建表扩展
            String sinkExtend = tableProcess.getSinkExtend();
            // 拼接状态中的key  source:operationType
            String key = sourceTable + ":" + operateType;

            // 判断是不是维度数据表的配置，如果是维度数据，提前在hbase中建表
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
                checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
            }
            // 将配置信息放到状态中
            ctx.getBroadcastState(mapStateDescriptor).put(key, tableProcess);
        }
    }

    /**
     * 完成通过Phoenix对维度数据进行建表
     */
    private void checkTable(String tableName, String fields, String pk, String ext) {
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }

        // 拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA +
                "." + tableName + "(");

        // 对sinkColumn进行切分 得到每个字段 id，tm_name
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if (field.equals(pk)) {
                createSql.append(field).append(" varchar primary key");
            } else {
                createSql.append(field).append(" varchar");
            }
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")").append(ext);
        System.out.println("建表语句为：" + createSql);

        // 创建预执行语句
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            // 执行sql语句
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在Phoenix中创建表失败");
        } finally {
            // 释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
