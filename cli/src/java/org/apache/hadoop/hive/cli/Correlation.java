package org.apache.hadoop.hive.cli;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Correlation {
	public String SHC_TYPE;
	public String SHC_NAME;
	public String SHC_TNAME;
	public int SHC_SID;
	public int SHC_DID;
	public String SHC_GRAN;
	public String SHC_MAPFUNC;
	public String SHC_INPUT_TYPE;
	public String SHC_OUTPUT_TYPE;
	public String SHC_STATUS;
	public int SHC_NNUM_VIOLATION;
	
	
	
	public Correlation(ResultSet rs){
		try{
			SHC_TYPE=rs.getString("SHC_TYPE");
			SHC_NAME=rs.getString("SHC_NAME");
			SHC_TNAME=rs.getString("SHC_TNAME");
			
			SHC_GRAN=rs.getString("SHC_GRAN");
			SHC_MAPFUNC=rs.getString("SHC_MAPFUNC");
		
			SHC_INPUT_TYPE=rs.getString("SHC_INPUT_TYPE");
			SHC_OUTPUT_TYPE=rs.getString("SHC_OUTPUT_TYPE");
			
			SHC_SID=rs.getInt("SHC_SID");
			SHC_DID=rs.getInt("SHC_DID");

			SHC_STATUS=rs.getString("SHC_STATUS");
			// if(SHC_STATUS.equals("D")){
			// 	SHC_NNUM_VIOLATION=0;
			// }else{

			// }
			
			
		}catch(Exception e){
			System.out.printf("consturction error \n");

			System.err.println("Caught IOException: " + e.getMessage());

		}
	}
	
	public boolean isSoft(){
		return SHC_TYPE.equals("Soft");
	} 

	public boolean waitForVerify(){
		return SHC_STATUS.equals("V");  // waiting for verification 
	}
	
	
	public String toString(){

		String resultFormat="SHC_TYPE: %s \n"
				+ "SHC_NAME: %s \n"
				+ "SHC_TNAME: %s \n"
				+ "SHC_SID: %d \n"
				+ "SHC_DID: %d \n"
				+"SHC_GRAN: %s \n"
				+"SHC_MAPFUNC:%s \n"
				+"SHC_INPUT_TYPE: %s \n"
				+"SHC_OUTPUT_TYPE:%s \n";
		
		String result=String.format(resultFormat, SHC_TYPE,
				SHC_NAME,SHC_TNAME,SHC_SID,SHC_DID,SHC_GRAN,SHC_MAPFUNC,SHC_INPUT_TYPE,SHC_OUTPUT_TYPE);
		return result;
	}

	public String toQuery(){
		//SHC_NAME, SID, DID, SHC_GRAN, SHC_MAPFUNC, SHC_INPUT_TYPE, SHC_OUTPUT_TYPE
		String queryFormat="%s %d %d %s %s %s %s ";

		String result=String.format(queryFormat, SHC_NAME, SHC_SID, SHC_DID, SHC_GRAN, SHC_MAPFUNC, SHC_INPUT_TYPE, SHC_OUTPUT_TYPE);

		return result;
	}
}
