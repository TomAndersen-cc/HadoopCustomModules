package com.tomandersen.sinks;

import com.google.common.base.Strings;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author TomAndersen
 * @Date 2020/3/3
 * @Version
 * @Description
 */
public class CustomLoggerSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(CustomLoggerSink.class);
    public static final int DEFAULT_MAX_BYTE_DUMP = 16;
    private int maxBytesToLog = 16;
    public static final String MAX_BYTES_DUMP_KEY = "maxBytesToLog";

    public CustomLoggerSink() {
    }

    public void configure(Context context) {
        // 1.获取配置信息
        String strMaxBytes = context.getString("maxBytesToLog");
        if (!Strings.isNullOrEmpty(strMaxBytes)) {
            try {
                this.maxBytesToLog = Integer.parseInt(strMaxBytes);
            } catch (NumberFormatException var4) {
                logger.warn(String.format("Unable to convert %s to integer, using default value(%d) for maxByteToDump", strMaxBytes, 16));
                this.maxBytesToLog = 16;
            }
        }
    }

    public Status process() throws EventDeliveryException {
        // 定义Event状态
        Status result = Status.READY;
        // 1.获取当前Channel信道对象
        Channel channel = this.getChannel();
        // 2.获取当前Transaction事务对象
        Transaction transaction = channel.getTransaction();
        // 定义当前Event事件
        Event event = null;

        // 在try{}catch{}finally{}中开启事务、处理Event事件、提交事务、关闭事务
        try {
            // 3.开启事务,然后处理Event
            transaction.begin();
            // 4.获取Event事件
            event = channel.take();
            // 一定要对Event进行判空,因为一开始Channel中没有Event,即take()会返回值为null的Event
            // 5.处理Event
            if (event != null) {
                // 自定义内容:给Header插入maxBytesToLog字段
                event.getHeaders().put("maxBytesToLog", String.valueOf(this.maxBytesToLog));
                // 如果日志对象可以使用info级别日志,则打印日志
                if (logger.isInfoEnabled()) {
                    logger.info("Event: " + EventHelper.dumpEvent(event, this.maxBytesToLog));
                }
            } else {
                // 如果Event为空则将状态置为退避
                result = Status.BACKOFF;
            }
            // 当将Event处理完成之后提交Transaction事务
            // 6.提交事务
            transaction.commit();
        } catch (Exception var9) {
            // 如果存在异常事务需要回滚,即撤销此次Event处理
            // 6.存在异常则回滚事务
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, var9);
        } finally {
            // 7.关闭事务
            transaction.close();
        }

        // 8.返回Event处理结果
        return result;
    }
}
