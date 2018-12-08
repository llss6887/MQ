package com.liss.simple;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class ScheduledMessage {
    public static void main(String[] args) {
        //创建生产者，生产者组需要写
        DefaultMQProducer producer = new DefaultMQProducer("first_producer_group");
        //设置nameserver，多个用逗号分隔
        producer.setNamesrvAddr("localhost:7986,localhost:9875");
        for (int i = 0; i <10 ; i++) {
            try {
                Message message = new Message("test_topic", "*", ("RockMQ" + i).getBytes("UTF-8"));
                //这里设置  1代表延迟1s 2代表延迟5秒，3代表延迟10s 以此类推
                message.setDelayTimeLevel(3);
                SendResult result = producer.send(message);
                System.out.println(result);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        //关闭
        producer.shutdown();
    }
}
