package com.stock.hbase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import com.stock.dto.StockRealTimeData;
import com.stock.util.CommonUtil;
import com.stock.util.ScalaCommonUtil;

public class HbaseClientUtil {
	
	 public static String insertByObject(Object obj,String tableTmp,String familyTmp) throws Exception {

	        String tableName = tableTmp ;
	        String tableFamily = familyTmp;

	        Configuration conf = ScalaCommonUtil.getconf();


	        if(obj == null){
	            return "-1";
	        }
	        String rowKey = CommonUtil.obj2string(CommonUtil.getter(obj, "rowkey"));
	        if ("-1".equals(rowKey)) {
	            return "-1";
	        }
	        HTable table = new HTable(conf, tableName);
	        Put put = new Put(Bytes.toBytes(rowKey));
	        Field[] childFields = obj.getClass().getDeclaredFields();
	        Field[] superFields = obj.getClass().getSuperclass().getDeclaredFields();
	        List<Field> fields = new ArrayList<Field>();
	        for(Field field:childFields){
	        	fields.add(field);
	        }
	        for(Field field:superFields){
	        	fields.add(field);
	        }
	        
	        for (int i = 0; i < fields.size(); i++) {
	        	 Field field = fields.get(i);
	        	 field.setAccessible(true);
	           
	            String filedName = field.getName();
	            Class<?> type = field.getType();
	            String fieldSimpleTypeName = type.getSimpleName();
	            Object fieldValueTmp = CommonUtil.getter(obj, filedName);
	            if ("Long".equals(fieldSimpleTypeName)) {
	                Long fieldValue = CommonUtil.obj2long(fieldValueTmp);
	                if (fieldValue != null) {
	                    put.add(Bytes.toBytes(tableFamily), Bytes.toBytes(filedName), Bytes.toBytes(fieldValue));
	                }
	            } else if ("String".equals(fieldSimpleTypeName)) {
	                String fieldValue = CommonUtil.obj2string(fieldValueTmp);
	                if (StringUtils.isNotEmpty(fieldValue) && !fieldValue.equals("-1")) {
	                    put.add(Bytes.toBytes(tableFamily), Bytes.toBytes(filedName), Bytes.toBytes(fieldValue));
	                }
	            }else if ("Double".equals(fieldSimpleTypeName)) {
	                Double fieldValue = CommonUtil.obj2Double(fieldValueTmp);
	                if (fieldValue!=null) {
	                    put.add(Bytes.toBytes(tableFamily), Bytes.toBytes(filedName), Bytes.toBytes(fieldValue));
	                }
	            }
	        }
	        table.put(put);
	        table.flushCommits();
	        table.close();
	        return "1";
	    }
	 
	 
	 public static void main(String[] args) {
		 StockRealTimeData stockRealTimeData =new  StockRealTimeData();
		 stockRealTimeData.setRowkey("11");
		 try {
			insertByObject(stockRealTimeData, "aa", "aa");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
