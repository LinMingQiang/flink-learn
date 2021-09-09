package common.java.bean;

public class RestfulUrlParameter {
    // yarn
    public static String YARN_APPS_STATE = "/apps?state=";
    public static String YARN_APPS = "/apps";
    public static String FLINK_TASKMANAGER = "/taskmanagers";
    // flink

    public static String FLINK_STREAM_JOBS_OVERVIEW = "/jobs/overview";
    public static String FLINK_STREAM_JOBS = "/jobs/";
    public static String FLINK_VERTICES = "/vertices";
    public static String FLINK_JOBS_EXCEPTION(String yarnPre, String appid, String jid) {
        return yarnPre
                + "/"
                + appid
                + FLINK_STREAM_JOBS
                + jid
                + "/exceptions";
    }

    public static String FLINK_JOBS_CHECKPOINT(String yarnPre, String appid, String jid) {
        return yarnPre
                + "/"
                + appid
                + FLINK_STREAM_JOBS
                + jid
                + "/checkpoints";
    }

    public static String FLINK_ALL_TASK_MANAGER(String yarnPre, String appid) {
        return yarnPre
                + "/"
                + appid
                + FLINK_TASKMANAGER;
    }

    public static String FLINK_TASK_MANAGER(String yarnPre, String appid, String tmid) {
        return yarnPre
                + "/"
                + appid
                + FLINK_TASKMANAGER
                +"/"
                +tmid;
    }
    /**
     *  获取JID的具体信息，包括plan，source，sink等的输入输出，并发度，包含
     * @param yarnPre
     * @param appid
     * @param jid
     * @return
     */
    public static String FLINK_JOBS_JID_INFO(String yarnPre, String appid, String jid){
        return yarnPre
                + "/"
                + appid
                +FLINK_STREAM_JOBS
                +jid;
    }


    public static String FLINK_JOBS_JID_VERTICES_INFO(String yarnPre, String appid, String jid, String verticesId){
        return yarnPre
                + "/"
                + appid
                +FLINK_STREAM_JOBS
                + jid
                +FLINK_VERTICES
                +"/"
                +verticesId
                ;

    }


}
