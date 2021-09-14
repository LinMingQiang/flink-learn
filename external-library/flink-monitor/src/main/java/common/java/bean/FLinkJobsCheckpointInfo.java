package common.java.bean;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

public class FLinkJobsCheckpointInfo {
    public JSONObject summary;
    public JSONObject counts;
    public LatestCkpInfo latest;
    public java.util.List<CheckpointInfo> history;

    @Override
    public String toString() {
        return "FLinkJobsCheckpointInfo{" +
                "summary=" + summary +
                ", counts=" + counts +
                ", latest=" + latest +
                ", history=" + history +
                '}';
    }

    public FLinkJobsCheckpointInfo(JSONObject summary, JSONObject counts, LatestCkpInfo latest, List<CheckpointInfo> history) {
        this.summary = summary;
        this.counts = counts;
        this.latest = latest;
        this.history = history;
    }

    public class CheckpointInfo {
        public int id;

        @Override
        public String toString() {
            return "CheckpointInfo{" +
                    "id=" + id +
                    ", status='" + status + '\'' +
                    ", is_savepoint=" + is_savepoint +
                    ", trigger_timestamp=" + trigger_timestamp +
                    ", latest_ack_timestamp=" + latest_ack_timestamp +
                    ", state_size=" + state_size +
                    ", end_to_end_duration=" + end_to_end_duration +
                    ", alignment_buffered=" + alignment_buffered +
                    ", num_subtasks=" + num_subtasks +
                    ", num_acknowledged_subtasks=" + num_acknowledged_subtasks +
                    ", tasks=" + tasks +
                    ", external_path='" + external_path + '\'' +
                    ", discarded=" + discarded +
                    '}';
        }

        public CheckpointInfo(int id, String status, boolean is_savepoint, long trigger_timestamp, long latest_ack_timestamp, long state_size, int end_to_end_duration, int alignment_buffered, int num_subtasks, int num_acknowledged_subtasks, JSONObject tasks, String external_path, boolean discarded) {
            this.id = id;
            this.status = status;
            this.is_savepoint = is_savepoint;
            this.trigger_timestamp = trigger_timestamp;
            this.latest_ack_timestamp = latest_ack_timestamp;
            this.state_size = state_size;
            this.end_to_end_duration = end_to_end_duration;
            this.alignment_buffered = alignment_buffered;
            this.num_subtasks = num_subtasks;
            this.num_acknowledged_subtasks = num_acknowledged_subtasks;
            this.tasks = tasks;
            this.external_path = external_path;
            this.discarded = discarded;
        }

        public String status;
        public boolean is_savepoint;
        public long trigger_timestamp;
        public long latest_ack_timestamp;
        public long state_size;
        public int end_to_end_duration;
        public int alignment_buffered;
        public int num_subtasks;
        public int num_acknowledged_subtasks;
        public JSONObject tasks;
        public String external_path;
        public boolean discarded;
    }

    public class LatestCkpInfo{
        public LatestCkpInfo(CheckpointInfo completed, String savepoint, String failed, String restored) {
            this.completed = completed;
            this.savepoint = savepoint;
            this.failed = failed;
            this.restored = restored;
        }

        @Override
        public String toString() {
            return "LatestCkpInfo{" +
                    "completed=" + completed +
                    ", savepoint='" + savepoint + '\'' +
                    ", failed='" + failed + '\'' +
                    ", restored='" + restored + '\'' +
                    '}';
        }

        public CheckpointInfo completed;
        public String savepoint;
        public String failed;
        public String restored;
    }
}
