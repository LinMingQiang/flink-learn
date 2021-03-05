package com.pojo;

public class TestPoJo {

    public  String topic,msg;
    public Long ll;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public TestPoJo(String topic, String msg, Long ll) {
        this.topic = topic;
        this.msg = msg;
        this.ll = ll;
    }

    public TestPoJo() {
    }
}
