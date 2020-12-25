package com.flink.learn.bean;

public class ReportLogPojo {
    public String app_id;
    public Long reqTime = 0L;
    public Long impTime = 0L;
    public Long clickTime = 0L;
    public String req_id;
    public Long req;
    public Long imp;
    public Long click;

    public ReportLogPojo() {
    }

    @Override
    public String toString() {
        return "ReportLogPojo{" +
                "app_id='" + app_id + '\'' +
                ", reqTime=" + reqTime +
                ", impTime=" + impTime +
                ", clickTime=" + clickTime +
                ", req_id='" + req_id + '\'' +
                ", req=" + req +
                ", imp=" + imp +
                ", click=" + click +
                '}';
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public Long getReqTime() {
        return reqTime;
    }

    public void setReqTime(Long reqTime) {
        this.reqTime = reqTime;
    }

    public Long getImpTime() {
        return impTime;
    }

    public void setImpTime(Long impTime) {
        this.impTime = impTime;
    }

    public String getReq_id() {
        return req_id;
    }

    public Long getClickTime() {
        return clickTime;
    }

    public void setClickTime(Long clickTime) {
        this.clickTime = clickTime;
    }

    public void setReq_id(String req_id) {
        this.req_id = req_id;
    }

    public Long getReq() {
        return req;
    }

    public void setReq(Long req) {
        this.req = req;
    }

    public Long getImp() {
        return imp;
    }

    public void setImp(Long imp) {
        this.imp = imp;
    }

    public Long getClick() {
        return click;
    }

    public void setClick(Long click) {
        this.click = click;
    }

    public ReportLogPojo(String req_id, String app_id, Long reqTime, Long impTime, Long clickTime, Long req, Long imp, Long click) {
        this.app_id = app_id;
        this.impTime = impTime;
        this.req_id = req_id;
        this.req = req;
        this.imp = imp;
        this.click = click;
        this.reqTime = reqTime;
        this.clickTime = clickTime;
    }
}

