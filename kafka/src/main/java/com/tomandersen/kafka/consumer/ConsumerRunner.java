package com.tomandersen.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <h3>此类专门用于提供多线程MultiThread运行Consumer的方式.</h3>
 * 通过创建ConsumerRunner实例,将其放入Thread线程对象中并调用start()方法
 * 就将Consumer以多线程的方式启动
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/14
 * @see KafkaConsumer
 */
public class ConsumerRunner implements Runnable {
    // 提供原子操作的Boolean对象
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // Kafka Consumer
    private final Consumer consumer;
    // 批次缓冲区,每条消息作为一个缓冲对象
    private final List<String> batchBuffer;
    // 批次大小,即每批最多消费记录数
    private final int maxBatchSize;
    // 消费周期(second),即消费最长等待时间
    private final long maxIntervalMills;


    public ConsumerRunner(Consumer consumer, int maxBatchSize, long maxIntervalMills) {
        this.batchBuffer = new ArrayList<>();
        this.consumer = consumer;
        this.maxBatchSize = maxBatchSize;
        this.maxIntervalMills = maxIntervalMills;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>consumeFrom</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>consumeFrom</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            // 如果Consumer未订阅消息则打印日志,并且关闭当前Consumer
            if (consumer.subscription().size() == 0) {
                Logger logger = LoggerFactory.getLogger(ConsumerRunner.class);
                logger.warn("This kafka consumer doesn't subscribe any topic and will be closed!");
                shutdown();
            }
            // 获取当前系统时间
            long timer = System.currentTimeMillis();
            while (!closed.get()) {
                // 每1000ms获取一次消息
                ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(1000));
                // Handle new records
                // 将需要消费的消息存入缓冲区
                for (ConsumerRecord record : records) {
                    batchBuffer.add("topic:" + record.topic()
                            + ",partition:" + record.partition()
                            + ",offset:" + record.offset()
                            + ",key:" + record.key()
                            + ",value:" + record.value());
                }
                // 如果缓冲区达到最大批次大小或者非空却到达最大消费周期,则输出消息,并刷新缓冲区
                if (batchBuffer.size() >= maxBatchSize ||
                        (batchBuffer.size() != 0 && (System.currentTimeMillis() - timer) >= maxIntervalMills)) {
                    // 此处为具体的消费方式
                    // 可以打印到控制台,或者存入数据库等等
                    System.out.println(batchBuffer);
                    // 向Kafka集群手动提交Offset
                    consumer.commitAsync();
                    // 刷新批次缓冲区
                    batchBuffer.clear();
                    // 刷新计时器
                    timer = System.currentTimeMillis();
                }

            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }


    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
