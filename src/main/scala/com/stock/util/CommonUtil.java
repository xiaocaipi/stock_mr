package com.stock.util;


import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by caidanfeng733 on 4/29/15.
 */
public class CommonUtil {

    public static String obj2string(Object f) {
        String returnString = "";
        if (f instanceof Long) {
            long tmp1 = (Long) f;
            returnString = String.valueOf(tmp1);
        } else if (f instanceof Integer) {
            int tmp1 = (Integer) f;
            returnString = String.valueOf(tmp1);
        } else if (f instanceof BigDecimal) {
            BigDecimal tmp1 = (BigDecimal) f;
            returnString = tmp1.toString();
        } else if (f instanceof BigInteger) {
            BigInteger tmp1 = (BigInteger) f;
            returnString = tmp1.toString();
        } else {
            returnString = (String) f;
        }
        if (StringUtils.isEmpty(returnString)) {
            returnString = "-1";
        }
        return returnString;
    }

    public static Long obj2long(Object f) {
        if (f instanceof Long) {
            long tmp1 = (Long) f;
            return tmp1;
        }else{
            return null;
        }
    }
    
    public static Double obj2Double(Object f) {
        if (f instanceof Double) {
            double tmp1 = (Double) f;
            return tmp1;
        }else{
            return null;
        }
    }

    public static BigDecimal String2BigDecimal(String origin){
        if(origin == null){
            return new BigDecimal(0);
        }else{
            return new BigDecimal(origin);
        }
    }

    public static String convertNull(Object obj){
        if(obj ==null){
            return "";
        }else{
            return String.valueOf(obj);
        }
    }

    public static Object getter(Object obj, String att) throws Exception {
        Method method = obj.getClass().getMethod("get" + captureName(att));
        return method.invoke(obj);
    }

    public static void setter(Object obj, String att, Object value,
                              Class<?> type) {
        try {
            Method method = obj.getClass().getMethod("set" + captureName(att), type);
            method.invoke(obj, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String captureName(String name) {
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        return  name;

    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(new Date());
        return time;
    }
}
