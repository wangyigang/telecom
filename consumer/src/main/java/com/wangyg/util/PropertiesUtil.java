package com.wangyg.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * 使用properties用于初始化
 * <p>
 * 工具类常作为静态方法，作为工具类，供其他类使用
 */
public class PropertiesUtil {
    //用于获取properties 进行封装resources中读取到的属性信息

    public static Properties properties = null;

    //使用静态代码块进行初始化
    static {
        //先创建properties对象
        properties = new Properties();
        //使用getSystemResuouceAsStream作为流
        InputStream stream = ClassLoader.getSystemResourceAsStream("kafka.properties");
        try {
            //使用Load方法加载stream流，加载.properties 中的内容
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //封装一个方法专门用于获取数据的value
    public static  String getPropertiesValue(String keyValue){
        //可以使用get()方法--或者使用getProperty()
        return properties.getProperty(keyValue);
    }


}
