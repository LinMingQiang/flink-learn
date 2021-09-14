package com.flink.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class AkkaInvocationHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        System.out.println(method.getDeclaringClass() + ">>>>" + method.getName());
        return null;
    }
}
