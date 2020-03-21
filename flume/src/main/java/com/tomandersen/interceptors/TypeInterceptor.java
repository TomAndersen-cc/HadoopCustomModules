package com.tomandersen.interceptors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @Author TomAndersen
 * @Date 2020/3/3
 * @Version
 * @Description: 此class属于自定义实现flume_Interceptor, 作用是根据Event中Body的指定字段向Event的Header中
 * 插入不同的键值对,即对Event标记类别
 */
public class TypeInterceptor implements Interceptor {


    // 初始化时可以不做操作
    public void initialize() {
        // Do nothing
    }


    // 单个Event拦截
    public Event intercept(final Event event) {
        // 1.获取Event中的Header
        Map<String, String> headers = event.getHeaders();

        // 2.获取Event中的Body,将其转换成字符串String
        String body = new String(event.getBody());

        // 3.根据Body中数据向Header添加键值对,表明日志类型
        // 如果Body中包含"start"则说明当前Event为启动日志
        if (body.contains("start")) {
            // 4.添加Header信息
            headers.put("type", "Startup");
            //否则为事件日志
        } else {
            // 4.添加Header信息
            headers.put("type", "Event");
        }
        return event;
    }


    // 批量Event拦截
    // 注意:既可以原Event集合进行修改,也可以创建新的Event集合作为成员变量,将此成员变量返回
    public List<Event> intercept(final List<Event> events) {
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


    // 关闭时可以不作操作
    public void close() {
        // Do nothing
    }


    // 创建静态内部类实现Interceptor.Builder接口
    public static class Builder implements Interceptor.Builder {

        // 定义配置信息
        private Context context;

        // 定义Interceptor生成器
        public Interceptor build() {
            return new TypeInterceptor();
        }

        // 获取配置信息
        public void configure(Context context) {
            this.context = context;
        }
    }

}
