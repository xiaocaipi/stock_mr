package com.stock;

import java.util.List;

import com.stock.util.HbaseClientUtil;
import com.stock.vo.StockAlertVo;

import util.FileMyUtil;

public class app1 { 
	
	public static void main(String[] args) throws Exception {
		String filePath = "/home/caidanfeng733/stock/rt/2016-06-23";
		String tmpcontent = FileMyUtil.readFile(filePath);
		String [] contents = tmpcontent.split("\n");
		for(String content :contents){
			System.out.println(content);
		}
	}

}
