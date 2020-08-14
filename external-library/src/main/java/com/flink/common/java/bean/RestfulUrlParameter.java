package com.flink.common.java.bean;

public class RestfulUrlParameter {
    // yarn
    public static String YARN_REST_PREFIX = "/ws/v1/cluster";
    public static String YARN_APPS_STATE = "/apps?state=";
    public static String YARN_APPS = "/apps";
    public static String FLINK_TASKMANAGER = "/taskmanagers";
    // flink
    public static String FLINK_STREAM_JOB = "/jobs/overview";

    public static String FLINK_JOBS_EXCEPTION(String yarnPre, String appid, String jid) {
        return yarnPre
                + "/"
                + appid
                + "/jobs/"
                + jid
                + "/exceptions";
    }

    public static String FLINK_JOBS_CHECKPOINT(String yarnPre, String appid, String jid) {
        return yarnPre
                + "/"
                + appid
                + "/jobs/"
                + jid
                + "/checkpoints";
    }

    public static String FLINK_TASK_MANAGER(String yarnPre, String appid) {
        return yarnPre
                + "/"
                + appid
                + FLINK_TASKMANAGER;
    }

}
