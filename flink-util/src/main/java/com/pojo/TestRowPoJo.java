package com.pojo;

import org.apache.flink.table.annotation.DataTypeHint;

public class TestRowPoJo {

    public  String topic,msg;
    public Long offsets;
    @DataTypeHint("RAW")
    public  DayMonthHour oay_month_hour;

    @Override
    public String toString() {
        return "TestRowPoJo{" +
                "topic='" + topic + '\'' +
                ", msg='" + msg + '\'' +
                ", offsets=" + offsets +
                ", oay_month_hour=" + oay_month_hour +
                '}';
    }

    public Long getOffsets() {
        return offsets;
    }

    public void setOffsets(Long offsets) {
        this.offsets = offsets;
    }

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


    public DayMonthHour getoay_month_hour() {
        return oay_month_hour;
    }

    public void setoay_month_hour(DayMonthHour oay_month_hour) {
        this.oay_month_hour = oay_month_hour;
    }

    public TestRowPoJo(String topic, String msg, Long offsets, DayMonthHour oay_month_hour) {
        this.topic = topic;
        this.msg = msg;
        this.offsets = offsets;
        this.oay_month_hour = oay_month_hour;
    }

    public TestRowPoJo() {
    }
}
