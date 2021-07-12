package common.java.bean;

public class FlinkJobsExceptionInfo {
    public String root_exception;
    public String timestamp;
    public String all_exceptions;
    public String truncated;

    @Override
    public String toString() {
        return "FlinkJobsExceptionInfo{" +
                "root_exception='" + root_exception + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", all_exceptions='" + all_exceptions + '\'' +
                ", truncated='" + truncated + '\'' +
                '}';
    }

    public FlinkJobsExceptionInfo(String root_exception, String timestamp, String all_exceptions, String truncated) {
        this.root_exception = root_exception;
        this.timestamp = timestamp;
        this.all_exceptions = all_exceptions;
        this.truncated = truncated;
    }
}
