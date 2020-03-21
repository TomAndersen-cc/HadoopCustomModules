package com.tomandersen.kafka.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * 此类主要用于为KafkaStreams实现中间的处理过程.
 * (已测试)
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/20
 */
public class LogProcessor implements Processor<byte[], byte[]> {
    // 定义配置信息
    private ProcessorContext context;

    // 初始化获取配置信息
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    // 正式的处理逻辑
    // 本次实现的主要处理功能是将value中的">>>"标记清除
    @Override
    public void process(byte[] key, byte[] value) {

        // 将输入的字节数组转化成String对象
        String inputValue = new String(value);
        // 清除字符串中的指定字符
        inputValue = inputValue.replaceAll(">>>", "");
        // 将处理后的<Key Value>输出到上下文对象Context中,之后会传输给下一个Topic
        context.forward(key, inputValue.getBytes());

    }

    // 释放相关资源
    @Override
    public void close() {

    }
}
