package com.stock.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

/**
 * 将两个字段拼接起来（使用指定的分隔符）
 * @author Administrator
 *继承UDF3  因为要传进来3个参数
 */
public class Second2MinuteUDF implements UDF1<String, String> {

	private static final long serialVersionUID = 1L;
	
	@Override
	public String call(String origin) throws Exception {
		
		String a1 = origin.split(":")[0];
		String a2 = origin.split(":")[1];
		
		return a1+":"+a2;
		
	}

}
