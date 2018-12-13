package com.stock.util;

import com.stock.vo.StockData;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortUtil {

    public static List<String> sortCollection(List<String> list) {
        Collections.sort(list, new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                String part1witho1 = o1.split("--")[0];
                String part1witho2 = o2.split("--")[0];
                int comparavalue = new BigDecimal(part1witho1).compareTo(new BigDecimal(part1witho2));
                return comparavalue;
            }
        });
        return list;
    }


    public static List<StockData> sortStockDataCollection(List<StockData> list) {
        Collections.sort(list, new Comparator<StockData>() {

            @Override
            public int compare(StockData o1, StockData o2) {
                String value1 = o1.getDate();
                String value2 = o2.getDate();
                int comparavalue = value1.compareTo(value2);
                return comparavalue;
            }
        });
        return list;
    }





}
