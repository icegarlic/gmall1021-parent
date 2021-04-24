package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询Phoenix的工具类
 */
public class PhoenixUtil {
    private static Connection conn;
    public static void initConnection(){
        try {
            //注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //获取连接
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //设置操作的表空间
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //从Phoenix中查询数据
    public static <T>List<T> queryList(String sql,Class<T> clzz){
        if(conn == null){
            initConnection();
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> resList = new ArrayList<>();

        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            //获取结果集的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //获取结果集的列的数量
            int columnCount = metaData.getColumnCount();
            /*
            //处理结果集
                id      tm_name
                14      cc
                15      qq
            */
            while(rs.next()){
                //创建要封装的目标类型对象
                T obj = clzz.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    //给对象的属性赋值
                    BeanUtils.setProperty(obj,columnName,rs.getObject(i));
                }
                resList.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //释放资源
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return resList;
    }

    public static void main(String[] args) {
        List<JSONObject> list = queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class);
        System.out.println(list);
    }
}

