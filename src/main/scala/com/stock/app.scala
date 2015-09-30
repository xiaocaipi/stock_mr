package com.stock

import java.math.BigDecimal
import com.stock.util.HbaseClientUtil
import com.stock.vo.StockAlertVo

object app {
  
  def main(args: Array[String]) {
//    var states = scala.collection.mutable.Map[String, String]()
//    println (states.get("aa") == None)
//    val aa1 = new BigDecimal(12)
//    val aa2 = new BigDecimal(13)
//    println(aa1.compareTo(aa2))
    
     val alertVo = new StockAlertVo()
    alertVo.setCode("000723")
    alertVo.setStatus("1")
    HbaseClientUtil.insertByObject(alertVo, "test_alert", "base_cf")
    
}

}