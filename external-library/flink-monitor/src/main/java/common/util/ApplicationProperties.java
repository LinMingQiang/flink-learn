package common.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class ApplicationProperties {
    public static String PROPER_FILE_NAME = "application.properties";
    Properties prop = null;
    public ApplicationProperties() throws IOException {
        prop = new Properties();
        InputStreamReader propIn = new InputStreamReader(ApplicationProperties.class
                .getClassLoader().getResourceAsStream(PROPER_FILE_NAME), "UTF-8");
        prop.load(propIn);
        propIn.close();
    }

    public static void main(String[] args) throws IOException {
        ApplicationProperties p = new ApplicationProperties();
        p.prop.entrySet().forEach(x -> System.out.println(x));
    }

}
