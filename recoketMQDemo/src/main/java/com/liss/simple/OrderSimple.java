package com.liss.simple;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import javax.swing.*;
import java.util.List;

public class OrderSimple {

    public static void main(String[] args) throws Exception {
        orderProduce();
        orderConsumer();
    }

    private static void orderConsumer() throws Exception {
        //创建消费者
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer");
        //第一个参数  为topic的名称，第二个为 标签，取出TagA和TagB
        consumer.setNamesrvAddr("localhost:7986,localhost:9875");
        consumer.subscribe("order_topic","TagA || TagB");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                consumeOrderlyContext.setAutoCommit(false);
                for (MessageExt messageExt : list) {
                    System.out.printf(String.valueOf(messageExt.getBody()));
                }
                //返回消费状态
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
    }

    private static void orderProduce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_produce");
        producer.setNamesrvAddr("localhost:7986,localhost:9875");
        producer.start();
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 100; i++) {
            //模拟订单ID
            int orderId = i % 10;
            Message msg = new Message("order_topic",tags[i % tags.length], "KEY" + i,
                    ("Hello RocketMQ " + i).getBytes("UTF-8"));
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer id = (Integer) o;
                    int index = id % list.size();
                    return list.get(index);
                }
            }, orderId);
        }
    }
}
