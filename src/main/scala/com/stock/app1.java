package com.stock;

import java.util.List;

import com.stock.util.HbaseClientUtil;
import com.stock.vo.StockAlertVo;

public class app1 {
	
	public static void main(String[] args) throws Exception {
		 List<StockAlertVo> list = HbaseClientUtil.getStockAlertList();
		 System.out.println(list.size());
	}

}
