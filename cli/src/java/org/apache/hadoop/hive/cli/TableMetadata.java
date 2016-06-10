package org.apache.hadoop.hive.cli;

import java.util.HashMap;
import java.util.Map;


public class TableMetadata {
	
	private static Map<Integer, String> columns=new HashMap<Integer,String>();
	
	
	public static void append(Integer index, String columnName){
		
		columns.put(index, columnName);
	}
	

	// public static void append
	public static String getColumnNameByIndex(Integer index){
		return columns.get(index);
	}

	//
	public String toString(){
		return String.format("%d:%s \n", 0, columns.get(0));
	}

	// done 
}