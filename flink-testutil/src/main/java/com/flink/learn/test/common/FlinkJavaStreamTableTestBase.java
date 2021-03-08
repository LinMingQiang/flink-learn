package com.flink.learn.test.common;
import com.core.FlinkSourceBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

public class FlinkJavaStreamTableTestBase extends FlinkSourceBuilder implements Serializable {

    @Before
    public void before() throws Exception {
        init();
    }

    @After
    public void after() {
        System.out.println("Test End");
    }

}
