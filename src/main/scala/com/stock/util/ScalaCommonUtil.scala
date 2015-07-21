package com.lufax.util

import java.io.File
import java.util.ResourceBundle
import java.util.Locale

object ScalaCommonUtil {
  
  def convertNull(obj:Any)  = {
    if(obj ==null)
      ""
    else
      obj.toString
  
}
  
  def getPropertyValue(key:String) :String = {
	  	var propFileName = new File("stock_mr_env.properties").getName();
		val index = propFileName.indexOf(".properties");
		if (index > 0) {
			propFileName = propFileName.substring(0, index);
		}
		val rsb = ResourceBundle.getBundle(propFileName, Locale.US);
		val url = rsb.getString(key);
	    url
  }
  
  def main(args: Array[String]) {
    
    println(getPropertyValue("UCS_APP_DB_DRIVER"))
}

}