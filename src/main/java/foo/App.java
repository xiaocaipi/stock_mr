package foo;

import java.io.IOException;

import util.FileMyUtil;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	
    	String filePath = "/home/caidanfeng733/stock/rt/2016-06-23";
		String tmpcontent = FileMyUtil.readFile(filePath);
		String [] contents = tmpcontent.split("\n");
		for(String content :contents){
			System.out.println(content);
		}
        
    }
}
