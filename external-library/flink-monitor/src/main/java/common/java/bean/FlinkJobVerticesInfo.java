package common.java.bean;

import com.alibaba.fastjson.JSONObject;

public class FlinkJobVerticesInfo {
    public String id;
    public String name;
    public int parallelism;
    public JSONObject metrics;

    public FlinkJobVerticesInfo(String id, String name, int parallelism, JSONObject metrics) {
        this.id = id;
        this.name = name;
        this.parallelism = parallelism;
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        return "FlinkJobVerticesInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", parallelism=" + parallelism +
                ", metrics=" + metrics +
                '}';
    }
}
