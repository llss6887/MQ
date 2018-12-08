package com.liss.simple.filter;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.*;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.List;

public class FilterSimple {
    public static void main(String[] args) throws Exception {
        filterProduce();
        filterConsumer();
    }

    private static void filterConsumer() throws Exception {
        //创建消费者
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer");
        //第一个参数  为topic的名称，第二个为 标签，取出TagA和TagB
        consumer.setNamesrvAddr("localhost:7986,localhost:9875");
        String code = MixAll.file2String("MyFilter.java");
        consumer.subscribe("order_topic", code);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                return null;
            }
        });
    }

    private static void filterProduce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_produce");
        producer.setNamesrvAddr("localhost:7986,localhost:9875");
        producer.start();
        Message msg = new Message("order_topic","*", "KEY",
                    ("Hello RocketMQ ").getBytes("UTF-8"));
        msg.putUserProperty("a", "20");
        producer.send(msg);
        producer.shutdown();
    }
}
