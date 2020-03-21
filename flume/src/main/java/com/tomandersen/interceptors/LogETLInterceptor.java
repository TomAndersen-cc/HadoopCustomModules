package com.tomandersen.interceptors;

import com.tomandersen.util.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @Author TomAndersen
 * @Date 2020/3/7
 * @Version
 * @Description: 用于对日志Event进行简单的ETL
 */
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    // 单个Event处理
    @Override
    public Event intercept(Event event) {
        // 1.获取Event中的Header和Body数据
        Map<String, String> headers = event.getHeaders();
        // 使用UTF-8编码方案解码字节数组
        String log = new String(event.getBody(), Charset.forName("UTF-8"));

        // 2.格式校验:启动日志(json)，事件日志(timestamp|json)
        // 使用定义的工具类进行日志格式解析,判断格式是否正确
        if (log.contains("start")) {

            // 如果是启动日志,使用专门的工具类进行解析判断
            if (LogUtils.verifyStartLog(log)) {

                // 如果验证成功,则直接返回当前event
                return event;
            }

        } else {

            // 如果是事件日志,使用专门的工具类进行解析判断
            if (LogUtils.verifyEventLog(log)) {

                // 如果验证成功,则直接返回当前event
                return event;
            }

        }
        // 如果验证失败,则直接返回null
        return null;
    }

    // 批量Event处理
    @Override
    public List<Event> intercept(List<Event> events) {
        // 直接使用单个Event处理方法
        for (Event event : events) {
            // 1.对每个Event采用单个Event拦截的方式进行处理
            Event processedEvent = intercept(event);
            // 2.如果返回值为null,则将原始event从Event集合中清除
            if (processedEvent == null) events.remove(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        // 声明配置信息
        Context context;

        // 获取配置信息
        @Override
        public void configure(Context context) {
            this.context = context;
        }

        // 用于创建Interceptor对象
        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

    }
}
