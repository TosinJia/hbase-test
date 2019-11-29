package com.tosin.hbase.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    private static Properties properties = null;
    static{
        // 通过类加载器，获取resource目录中的配置文件
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过Key,获取value值
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
