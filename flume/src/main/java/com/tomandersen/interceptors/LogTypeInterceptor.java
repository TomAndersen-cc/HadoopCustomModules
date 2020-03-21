package com.tomandersen.interceptors;

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
 * @Description
 */
public class LogTypeInterceptor implements Interceptor {

    // 初始化时不做操作
    @Override
    public void initialize() {
        // Do nothing
    }

    // 单个Event拦截
    // 根据Body中的数据向Header中添加不同的键值对信息
    @Override
    public Event intercept(Event event) {

        // 1.获取Event中Header和Body
        Map<String, String> headers = event.getHeaders();
        // 使用UTF-8编码方案解码字节数组
        String bodyStr = new String(event.getBody(), Charset.forName("UTF-8"));

        // 2.根据Body中的内容向Header中插入不同的键值对
        // 如果包含"start"关键字则表明是启动日志,否则判定为事件日志
        if (bodyStr.contains("start")) {
            headers.put("topic", "topic_start");
        } else headers.put("topic", "topic_event");

        // 3.返回处理之后的Event
        return event;
    }

    // 批量Event拦截
    // 注意:既可以原Event集合进行修改,也可以创建新的Event集合变量,将需要保留的Event添加进新集合变量中并返回
    @Override
    public List<Event> intercept(List<Event> events) {
        // 直接通过单个Event拦截方法进行处理,如果返回值为null则将其从Event集合中去除
        // 遍历Event集合
        for (Event event : events) {
            // 1.对每个Event采用单个Event拦截的方式进行处理
            Event processedEvent = intercept(event);
            // 2.如果返回值为null,则将原始event从Event集合中清除
            if (processedEvent == null) events.remove(event);
        }
        // 3.返回处理结果
        return events;
    }


    // 关闭时不做操作
    @Override
    public void close() {
        // Do nothing
    }

    // 创建静态内部类实现Interceptor.Builder接口
    public static class Builder implements Interceptor.Builder {

        // 定义配置信息
        Context context;

        // 获取配置信息
        @Override
        public void configure(Context context) {
            this.context = context;
        }

        // 用于生成Interceptor
        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }
    }
}
