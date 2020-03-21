package com.tomandersen.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.math.NumberUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author TomAndersen
 * @Date 2020/3/7
 * @Version
 * @Description: 此类用于处理各种日志
 */
public final class LogUtils {
    // 禁用构造方法
    private LogUtils() {
    }

    // 校验启动日志格式(json)是否正确
    public static boolean verifyStartLog(String log) {
        // 1.判空
        if (log == null) return false;

        // 2.简单判断json格式是否符合要求:如果开头或者结尾不包含"{"则返回false
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")) return false;

        return true;
    }

    // 校验事件日志格式(timestamp|json)是否正确
    public static boolean verifyEventLog(String log) {
        // 1.判空
        if (log == null) return false;

        // 2.切割字符串
        String[] strings = log.split("\\|");

        // 3.获取当前系统时间
        long current = new Date().getTime();

        // 4.判断时间戳是否合格
        // 如果时间字符串长度不等于13 || 字符串不全为数字则返回false
        if (strings[0].length() != 13 || NumberUtils.isDigits(strings[0])) return false;

        // 5.简单判断json格式是否符合要求:如果开头或者结尾不包含"{"则返回false
        if (!strings[1].trim().startsWith("{")
                || !strings[1].trim().endsWith("}")) return false;

        return true;
    }
}
