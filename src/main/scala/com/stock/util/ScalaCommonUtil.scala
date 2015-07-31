package com.stock.util;


import java.io.File
import java.util.ResourceBundle
import java.util.Locale
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.security.UserGroupInformation

object ScalaCommonUtil {
  
  def convertNull(obj:Any)  = {
    if(obj ==null)
      ""
    else
      obj.toString
  
}
  
  def getPropertyValue(key:String) :String = {
	  	val  currentSystem =  System.getProperty("os.name")
	  	var propertiesName = ""
	  	if(currentSystem.toLowerCase().contains("linux")){
	  	  propertiesName ="stock_mr_linux_env.properties"
	  	}else{
	  	  propertiesName ="stock_mr_env.properties"
	  	}
	  	var propFileName = new File(propertiesName).getName();
		val index = propFileName.indexOf(".properties");
		if (index > 0) {
			propFileName = propFileName.substring(0, index);
		}
		val rsb = ResourceBundle.getBundle(propFileName, Locale.US);
		val url = rsb.getString(key);
	    url
  }
  
  def getconf() : Configuration = {
		val config = HBaseConfiguration.create()
		val krbconfPath = getPropertyValue("krbconfPath")
		val principal = getPropertyValue("principal")
		val keytabPath = getPropertyValue("keytabPath")
		config.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4,hadoop5")
		config.set("hbase.zookeeper.property.clientPort", "2181")
		config.set("hadoop.security.authentication", "kerberos")
		config.set("hbase.security.authentication", "kerberos")
		config.set("hbase.master.kerberos.principal", "hbase/_HOST@hadoop3")
		config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@hadoop3")	
		
        System.setProperty("java.security.krb5.conf", krbconfPath)
		UserGroupInformation.setConfiguration(config)
		UserGroupInformation.loginUserFromKeytab(principal,keytabPath)
	    config
}
  
  def main(args: Array[String]) {
    
    println(getPropertyValue("UCS_APP_DB_DRIVER"))
}

}