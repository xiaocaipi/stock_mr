package com.stock.util;

import java.text.SimpleDateFormat; 
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;

import com.stock.vo.HbaseColumnValueVo;
import com.stock.vo.HbaseQuery;
import com.stock.vo.StockAlertVo;

import scala.reflect.internal.Trees.This;
import service.BaseHbaseService;

public class HbaseClientUtil  extends BaseHbaseService { 
	
	
	public static String insertByObject(Object obj, String tableTmp,
			String familyTmp) throws Exception {
		
		return hbaseInsertByObject(obj, tableTmp, familyTmp);
		
	}
	
	public static Put obj2put(Object obj, String tableTmp, String familyTmp)
			throws Exception {
		
		return hbaseObj2put(obj, tableTmp, familyTmp);
		
	}
	
	public static List<StockAlertVo> getStockAlertList () throws Exception{
		HbaseQuery query = new HbaseQuery();
		List<HbaseColumnValueVo> list = new ArrayList<HbaseColumnValueVo>();
		HbaseColumnValueVo vo = new HbaseColumnValueVo();
		vo.setFamily("base_cf"); 
		vo.setValue("2");
		vo.setOp("3");
		vo.setColumn("status");
		list.add(vo);
		query.setType("1");
		query.setColumValueList(list);
		return queryByConditionList(query,  new StockAlertVo(), "test_alert");
	}
	
	

}
