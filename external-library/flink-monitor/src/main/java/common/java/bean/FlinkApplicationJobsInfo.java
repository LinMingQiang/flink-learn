package common.java.bean;

public class FlinkApplicationJobsInfo {
    public long duration;
    public long start_time;
    public String jid;
    public String name;
    public String state;
    public long end_time;
    public long last_modification;
    public FlinkJobsTaskInfo tasks;

    @Override
    public String toString() {
        return "FlinkJobsInfo{" +
                "duration=" + duration +
                ", start_time=" + start_time +
                ", jid='" + jid + '\'' +
                ", name='" + name + '\'' +
                ", state='" + state + '\'' +
                ", end_time=" + end_time +
                ", last_modification=" + last_modification +
                ", tasks=" + tasks +
                '}';
    }

    public FlinkApplicationJobsInfo(long duration, long start_time, String jid, String name, String state, long end_time, long last_modification, FlinkJobsTaskInfo tasks) {
        this.duration = duration;
        this.start_time = start_time;
        this.jid = jid;
        this.name = name;
        this.state = state;
        this.end_time = end_time;
        this.last_modification = last_modification;
        this.tasks = tasks;
    }

    public class FlinkJobsTaskInfo {
        public int running;
        public int canceling;
        public int canceled;
        public int total;
        public int created;
        public int scheduled;
        public int deploying;
        public int reconciling;
        public int finished;
        public int failed;

        @Override
        public String toString() {
            return "FlinkJobsTaskInfo{" +
                    "running=" + running +
                    ", canceling=" + canceling +
                    ", canceled=" + canceled +
                    ", total=" + total +
                    ", created=" + created +
                    ", scheduled=" + scheduled +
                    ", deploying=" + deploying +
                    ", reconciling=" + reconciling +
                    ", finished=" + finished +
                    ", failed=" + failed +
                    '}';
        }

        public FlinkJobsTaskInfo(int running, int canceling, int canceled, int total, int created, int scheduled, int deploying, int reconciling, int finished, int failed) {
            this.running = running;
            this.canceling = canceling;
            this.canceled = canceled;
            this.total = total;
            this.created = created;
            this.scheduled = scheduled;
            this.deploying = deploying;
            this.reconciling = reconciling;
            this.finished = finished;
            this.failed = failed;
        }
    }
}

