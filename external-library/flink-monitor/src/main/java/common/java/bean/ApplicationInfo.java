package common.java.bean;

public class ApplicationInfo {
    public String id = null;
    public String user = null;
    public String name = null;
    public String queue = null;
    public String state = null;
    public String finalStatus = null;
    public String trackingUI = null;
    public String trackingUrl = null;
    public String applicationType = null;

    public String priority;
    public long startedTime;
    public long finishedTime;
    public long elapsedTime;
    public String amContainerLogs;
    public String amHostHttpAddress;
    public String amRPCAddress;
    public int allocatedMB;
    public int allocatedVCores;
    public int runningContainers;
    public long memorySeconds;
    public long vcoreSeconds;
    public float queueUsagePercentage;
    public float clusterUsagePercentage;
    public long preemptedResourceMB;
    public long preemptedResourceVCores;
    public int numNonAMContainerPreempted;
    public int numAMContainerPreempted;
    public String logAggregationStatus;
    public boolean unmanagedApplication;
    public String appNodeLabelExpression;
    public String amNodeLabelExpression;

    @Override
    public String toString() {
        return "ApplicationInfo{" +
                "id='" + id + '\'' +
                ", user='" + user + '\'' +
                ", name='" + name + '\'' +
                ", queue='" + queue + '\'' +
                ", state='" + state + '\'' +
                ", finalStatus='" + finalStatus + '\'' +
                ", trackingUI='" + trackingUI + '\'' +
                ", trackingUrl='" + trackingUrl + '\'' +
                ", applicationType='" + applicationType + '\'' +
                ", priority='" + priority + '\'' +
                ", startedTime=" + startedTime +
                ", finishedTime=" + finishedTime +
                ", elapsedTime=" + elapsedTime +
                ", amContainerLogs='" + amContainerLogs + '\'' +
                ", amHostHttpAddress='" + amHostHttpAddress + '\'' +
                ", amRPCAddress='" + amRPCAddress + '\'' +
                ", allocatedMB=" + allocatedMB +
                ", allocatedVCores=" + allocatedVCores +
                ", runningContainers=" + runningContainers +
                ", memorySeconds=" + memorySeconds +
                ", vcoreSeconds=" + vcoreSeconds +
                ", queueUsagePercentage=" + queueUsagePercentage +
                ", clusterUsagePercentage=" + clusterUsagePercentage +
                ", preemptedResourceMB=" + preemptedResourceMB +
                ", preemptedResourceVCores=" + preemptedResourceVCores +
                ", numNonAMContainerPreempted=" + numNonAMContainerPreempted +
                ", numAMContainerPreempted=" + numAMContainerPreempted +
                ", logAggregationStatus='" + logAggregationStatus + '\'' +
                ", unmanagedApplication=" + unmanagedApplication +
                ", appNodeLabelExpression='" + appNodeLabelExpression + '\'' +
                ", amNodeLabelExpression='" + amNodeLabelExpression + '\'' +
                '}';
    }

    public ApplicationInfo(String id, String user, String name, String queue, String state, String finalStatus, String trackingUI, String trackingUrl, String applicationType, String priority, long startedTime, long finishedTime, long elapsedTime, String amContainerLogs, String amHostHttpAddress, String amRPCAddress, int allocatedMB, int allocatedVCores, int runningContainers, long memorySeconds, long vcoreSeconds, float queueUsagePercentage, float clusterUsagePercentage, long preemptedResourceMB, long preemptedResourceVCores, int numNonAMContainerPreempted, int numAMContainerPreempted, String logAggregationStatus, boolean unmanagedApplication, String appNodeLabelExpression, String amNodeLabelExpression) {
        this.id = id;
        this.user = user;
        this.name = name;
        this.queue = queue;
        this.state = state;
        this.finalStatus = finalStatus;
        this.trackingUI = trackingUI;
        this.trackingUrl = trackingUrl;
        this.applicationType = applicationType;
        this.priority = priority;
        this.startedTime = startedTime;
        this.finishedTime = finishedTime;
        this.elapsedTime = elapsedTime;
        this.amContainerLogs = amContainerLogs;
        this.amHostHttpAddress = amHostHttpAddress;
        this.amRPCAddress = amRPCAddress;
        this.allocatedMB = allocatedMB;
        this.allocatedVCores = allocatedVCores;
        this.runningContainers = runningContainers;
        this.memorySeconds = memorySeconds;
        this.vcoreSeconds = vcoreSeconds;
        this.queueUsagePercentage = queueUsagePercentage;
        this.clusterUsagePercentage = clusterUsagePercentage;
        this.preemptedResourceMB = preemptedResourceMB;
        this.preemptedResourceVCores = preemptedResourceVCores;
        this.numNonAMContainerPreempted = numNonAMContainerPreempted;
        this.numAMContainerPreempted = numAMContainerPreempted;
        this.logAggregationStatus = logAggregationStatus;
        this.unmanagedApplication = unmanagedApplication;
        this.appNodeLabelExpression = appNodeLabelExpression;
        this.amNodeLabelExpression = amNodeLabelExpression;
    }
}
