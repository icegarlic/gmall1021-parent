package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 维度数据写入Hbase(phoenix)
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    /**
     * 声明连接器
     */
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 获取连接器
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        super.invoke(jsonObj, context);
        // 获取目的表名
        String sinkTableName = jsonObj.getString("sink_table");

        // 获取data数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(sinkTableName, dataJsonObj);

//            创建数据库连接对象
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(upsertSql);
                ps.execute();
                // 注：phoenix手动提交事务（DML）语句
                conn.commit();
//                System.out.println("执行写入phoenix的sql语句：" + upsertSql);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向phoenix中写入数据失败");
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
        }
    }

    /**
     * 生成向Phoenix中插入数据的语句    "data":{"tm_name":"aaa","id":13}
     */
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA +
                "." + tableName + " (" + StringUtils.join(dataJsonObj.keySet(), ",")
                + ") values('" + StringUtils.join(dataJsonObj.values(), "','") + "')";
        return upsertSql;
    }
}
