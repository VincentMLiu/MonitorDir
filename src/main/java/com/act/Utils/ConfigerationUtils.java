package com.act.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;

public class ConfigerationUtils {

	public static Map<String, String> propertiesMap = new HashMap<String, String>();

	
	public static void init (String fileName){
		try {
			Properties pro = new Properties();
			BufferedReader  bf;
			if (isWindows()){
				InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
				bf = new BufferedReader(new InputStreamReader(resourceAsStream, "utf-8"));
				resourceAsStream.close();
			}else{
				File propertiesFile = new File(JarToolUtil.getJarDir() + File.separator + "conf" + File.separator + fileName);
				InputStream resourceAsStream = new FileInputStream(propertiesFile);
				bf = new BufferedReader(new InputStreamReader(resourceAsStream, "utf-8"));
			}

			pro.load(bf);
			Iterator<String> it=pro.stringPropertyNames().iterator();
			while(it.hasNext()){
				String key=it.next();
				propertiesMap.put(key, new String(pro.getProperty(key)));
				//System.out.println(key+":"+pro.getProperty(key));
				}

			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public static String get(String name, String defaultValue) {
		String ret = "";
			ret = propertiesMap.get(name);
			if(StringUtils.isBlank(ret)) {
				return defaultValue;
			}
			return ret.trim();
		}
	
	
	
	public static void initOutConf (String fileUrl){
		try {
			Properties pro = new Properties();
			
			File propertiesFile = new File(fileUrl);
			InputStream resourceAsStream = new FileInputStream(propertiesFile);
			BufferedReader  bf = new BufferedReader(new InputStreamReader(resourceAsStream, "utf-8"));
			pro.load(bf);
			Iterator<String> it=pro.stringPropertyNames().iterator();
			while(it.hasNext()){
				String key=it.next();
				propertiesMap.put(key, pro.getProperty(key));
				//System.out.println(key+":"+pro.getProperty(key));
				}
			resourceAsStream.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}


	/**
	 * 判断当前操作系统是不是window
	 *
	 * @return boolean
	 */
	public static boolean isWindows() {
		boolean flag = false;
		if (System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1) {
			flag = true;
		}
		return flag;
	}

	public static void main(String[] args) throws Exception {
		init("provinceBlackUrl.properties");

		String userName = propertiesMap.get("fileType");  
		
		System.out.println(userName);
	}
	
}
