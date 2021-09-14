package com.flink.proxy;

import com.flink.rpc.JobManager;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcUtils;

import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;

public class ProxyTest {
    public static void main(String[] args) {
        JobManager jm = new JobManager();
        HashSet<Class<? extends RpcGateway>> interfaces = new HashSet<>();
        for (Class<?> interfaze : jm.getClass().getInterfaces()) {
            if (RpcGateway.class.isAssignableFrom(interfaze)) {
                System.out.println(interfaze);
                interfaces.add((Class<? extends RpcGateway>) interfaze);
            }
        }

        Set<Class<?>> implementedRpcGateways = new HashSet<>(interfaces);

        DispatcherGateway server =
                (DispatcherGateway)Proxy.newProxyInstance(
                ProxyTest.class.getClassLoader(),
                implementedRpcGateways.toArray(
                        new Class<?>[implementedRpcGateways.size()]),
                new AkkaInvocationHandler());
        System.out.println(server);
        server.submitJob(null, null);
    }
}
