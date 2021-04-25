package com.atguigu.gmall.realtime.utils;


import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 日期转换工具
 * JDK8的DateTimeFormatter替换SimpleDateFormat，因为SimpleDateFormat存在线程安全问题
 */
public class DateTimeUtil {
    private final static DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Date类型转换成字符串
     */
    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return FORMATTER.format(localDateTime);
    }

    /**
     * 字符串时间转换成时间戳
     */
    public static Long toTs(String YmdHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmdHms, FORMATTER);
        long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }
}
