package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.common.GmallConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询Phoenix的工具类
 */
public class PhoenixUtil {
    public static Connection conn = null;

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if (conn == null) {
            queryInit();
        }
        ArrayList<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 0; i < md.getColumnCount(); i++) {

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }


}
