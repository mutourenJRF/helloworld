package com.jd.jr.recommender.mq;

import java.io.IOException;
import java.util.Properties;

public class PropertyUtil {

	private static PropertyUtil instance;
	private Properties properties;
	
	private PropertyUtil() {
		properties = new Properties();
		try {
			properties.load(PropertyUtil.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static PropertyUtil getInstance(){
		if(instance == null){
			instance = new PropertyUtil();
		}
		return instance;
	}
	
	/**
	 * 获取属性
	 * @param key
	 * @return
	 */
	public String getProperty(String key){
		return this.properties.getProperty(key);
	}
	
}
