package com.flink.common.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertyUtils {
    public static String PROPER_FILE_NAME = "application.properties";
    Properties prop = null;
    public PropertyUtils() throws IOException {
        prop = new Properties();
        InputStreamReader propIn = new InputStreamReader(PropertyUtils.class
                .getClassLoader().getResourceAsStream(PROPER_FILE_NAME), "UTF-8");
        prop.load(propIn);
        propIn.close();
    }

    public static void main(String[] args) throws IOException {
        PropertyUtils p = new  PropertyUtils();
        p.prop.entrySet().forEach(x -> System.out.println(x));
    }

}
