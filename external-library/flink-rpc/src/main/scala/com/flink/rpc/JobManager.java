package com.flink.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class JobManager implements DispatcherGateway {
    @Override
    public String getAddress() {
        return null;
    }

    @Override
    public String getHostname() {
        return null;
    }

    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return null;
    }

    @Override
    public DispatcherId getFencingToken() {
        return null;
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(JobID jobId, Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        return null;
    }
}
