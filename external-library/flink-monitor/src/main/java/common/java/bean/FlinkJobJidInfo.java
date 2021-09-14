package common.java.bean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

public class FlinkJobJidInfo {
    public java.util.List<FlinkJobVerticesInfo> vertices ;
    public String jid;
    public String state;
    public JSONObject plan ;

    public FlinkJobJidInfo(List<FlinkJobVerticesInfo> vertices, String jid, String state, JSONObject plan) {
        this.vertices = vertices;
        this.jid = jid;
        this.state = state;
        this.plan = plan;
    }

    @Override
    public String toString() {
        return "FlinkJobJidInfo{" +
                "vertices=" + vertices +
                ", jid='" + jid + '\'' +
                ", state='" + state + '\'' +
                ", plan=" + plan +
                '}';
    }
}
