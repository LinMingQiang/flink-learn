package com.pojo;

public class KafkaTopicOffsetMsgPoJo {
    public String topic = "";
    public String msg = "";
    public long offset = 0L;

    public KafkaTopicOffsetMsgPoJo(String topic, String msg, long offset) {
        this.topic = topic;
        this.msg = msg;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KafkaTopicOffsetMsgPoJo{" +
                "topic='" + topic + '\'' +
                ", msg='" + msg + '\'' +
                ", offset=" + offset +
                '}';
    }

    public KafkaTopicOffsetMsgPoJo() {
    }
}
