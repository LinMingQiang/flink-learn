package common.java.bean;

import com.alibaba.fastjson.JSONObject;

import java.util.Objects;

public class FlinkTaskManagerInfo {
    public String id;
    public String path;
    public int dataPort;
    public long timeSinceLastHeartbeat;
    public int slotsNumber;
    public int freeSlots;
    public JSONObject hardware;

    @Override
    public String toString() {
        return "FlinkTaskManagerInfo{" +
                "id='" + id + '\'' +
                ", path='" + path + '\'' +
                ", dataPort=" + dataPort +
                ", timeSinceLastHeartbeat=" + timeSinceLastHeartbeat +
                ", slotsNumber=" + slotsNumber +
                ", freeSlots=" + freeSlots +
                ", hardware=" + hardware +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlinkTaskManagerInfo)) return false;
        FlinkTaskManagerInfo that = (FlinkTaskManagerInfo) o;
        return dataPort == that.dataPort &&
                timeSinceLastHeartbeat == that.timeSinceLastHeartbeat &&
                slotsNumber == that.slotsNumber &&
                freeSlots == that.freeSlots &&
                Objects.equals(id, that.id) &&
                Objects.equals(path, that.path) &&
                Objects.equals(hardware, that.hardware);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, path, dataPort, timeSinceLastHeartbeat, slotsNumber, freeSlots, hardware);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getDataPort() {
        return dataPort;
    }

    public void setDataPort(int dataPort) {
        this.dataPort = dataPort;
    }

    public long getTimeSinceLastHeartbeat() {
        return timeSinceLastHeartbeat;
    }

    public void setTimeSinceLastHeartbeat(long timeSinceLastHeartbeat) {
        this.timeSinceLastHeartbeat = timeSinceLastHeartbeat;
    }

    public int getSlotsNumber() {
        return slotsNumber;
    }

    public void setSlotsNumber(int slotsNumber) {
        this.slotsNumber = slotsNumber;
    }

    public int getFreeSlots() {
        return freeSlots;
    }

    public void setFreeSlots(int freeSlots) {
        this.freeSlots = freeSlots;
    }

    public JSONObject getHardware() {
        return hardware;
    }

    public void setHardware(JSONObject hardware) {
        this.hardware = hardware;
    }

    public FlinkTaskManagerInfo() {
    }

    public FlinkTaskManagerInfo(String id, String path, int dataPort, long timeSinceLastHeartbeat, int slotsNumber, int freeSlots, JSONObject hardware) {
        this.id = id;
        this.path = path;
        this.dataPort = dataPort;
        this.timeSinceLastHeartbeat = timeSinceLastHeartbeat;
        this.slotsNumber = slotsNumber;
        this.freeSlots = freeSlots;
        this.hardware = hardware;
    }
}
