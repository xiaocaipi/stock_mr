package com.stock;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.stock.util.SortUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import com.stock.util.HbaseClientUtil;
import com.stock.vo.StockAlertVo;

import util.FileMyUtil;

public class app1 {

    public static void main(String[] args) throws Exception {

        List<String> list = new ArrayList<String>();
        list.add("0.1--a1");
        list.add("0.2--a2");
        list.add("0.5--a3");
        list.add("0.3--a4");
        System.out.println(list);
        list  = SortUtil.sortCollection(list);

        System.out.println(list);

        list = list.subList(0,2);
        System.out.println(list);


    }

}
