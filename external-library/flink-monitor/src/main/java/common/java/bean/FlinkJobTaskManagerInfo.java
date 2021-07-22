package common.java.bean;

import com.alibaba.fastjson.JSONObject;

public class FlinkJobTaskManagerInfo {
    public String id ;
    public JSONObject metrics;

    public FlinkJobTaskManagerInfo(String id, JSONObject metrics) {
        this.id = id;
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        return "FlinkJobTaskManagerInfo{" +
                "id='" + id + '\'' +
                ", metrics=" + metrics +
                '}';
    }

    public FlinkJobTaskManagerInfo(JSONObject tmInfo) {
        if(tmInfo.containsKey("metrics")){
            metrics = tmInfo.getJSONObject("metrics");
        }
    }

    public Long getHeapMax(){
        return metrics.getLong("heapMax");
    }


    public Long getHeapUsed(){
        return metrics.getLong("heapUsed");
    }


    public Long getRemainHeap(){
        return (getHeapUsed() - getHeapMax());
    }
}
