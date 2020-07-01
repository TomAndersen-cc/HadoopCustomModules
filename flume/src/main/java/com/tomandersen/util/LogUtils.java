package com.tomandersen.util;

import org.apache.commons.lang.math.NumberUtils;

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
        // 1 切割
        String[] logContents = log.split("\\|");

        // 2 校验
        if(logContents.length != 2){
            return false;
        }

        //3 校验服务器时间
        if (logContents[0].length()!=13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }

        // 4 简单校验json格式
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){
            return false;
        }

        return true;

    }
}
