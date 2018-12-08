package com.liss.simple.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * 此文件或上传，不允许有中文 包括注释
 */
public class MyFilter implements MessageFilter {
    @Override
    public boolean match(MessageExt messageExt) {
        String a = messageExt.getUserProperty("a");
        return true;
    }
}
