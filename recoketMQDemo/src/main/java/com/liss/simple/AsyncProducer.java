package com.liss.simple;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class AsyncProducer {
    public static void main(String[] args) {
        //创建生产者，生产者组需要写
        DefaultMQProducer producer = new DefaultMQProducer("first_producer_group");
        producer.setNamesrvAddr("localhost:7986,localhost:9875");
        //重试
        producer.setRetryTimesWhenSendFailed(0);
        try {
        producer.start();
            for (int i = 0; i <10 ; i++) {
                Message message = new Message("test_topic", "*", ("RockMQ" + i).getBytes("UTF-8"));
                producer.send(message, new SendCallback() {
                    //成功
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult.getMsgId());
                    }
                    //失败
                    @Override
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        //关闭
        producer.shutdown();
    }
}
