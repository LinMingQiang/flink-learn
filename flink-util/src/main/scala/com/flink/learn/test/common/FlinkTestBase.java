package com.flink.learn.test.common;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;
public class FlinkTestBase extends AbstractTestBase implements Serializable {
    public static ExecutionEnvironment bEnv = null;
    @Before
    public void before(){
        System.out.println(">>>>>>>>>>>>>>>>>>>");
        bEnv = ExecutionEnvironment.getExecutionEnvironment();
    }
    @After
    public void after(){
        System.out.println("<<<<<<<<<<<<<<<<<<<");
    }

}
