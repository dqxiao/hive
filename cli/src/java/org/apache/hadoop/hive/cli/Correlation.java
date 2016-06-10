package org.apache.hadoop.hive.cli;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


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
	public String SHC_SVal; // used for constructed query 
	public String SHC_TargetVal; // to get result 

	public boolean SHC_VIOLATED;  // whether the SVAL in distinct violated val 
	
	
	public enum GRANTYPE {
		Singleton, Range, List;
	}
	
	
	// used for just debugging 
	public Correlation(){
		SHC_TYPE="Soft";
		SHC_NAME="test_correlation";
		SHC_TNAME="test";
		SHC_SID=0;
		SHC_DID=1;
		SHC_GRAN="singleton";
		SHC_MAPFUNC="mapping";
		SHC_INPUT_TYPE="int";
		SHC_OUTPUT_TYPE="float";
		
	}



	
	
	
	
	
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
			SHC_SVal="";
			SHC_VIOLATED=false;  
			//
			
		}catch(Exception e){
			System.out.printf("consturction error \n");

			System.err.println("Caught IOException: " + e.getMessage());

		}
	}
	
	public boolean isNullSval(){
		return SHC_SVal.equals("");
	}
	
	public boolean isSoft(){
		return SHC_TYPE.equals("Soft");
	} 

	public boolean waitForVerify(){
		return SHC_STATUS.equals("V");  // waiting for verification 
	}
	
	public GRANTYPE getGranType(){
		if(SHC_GRAN.equals("list")){
			return GRANTYPE.List;
		}
		if(SHC_GRAN.equals("range")){
			return GRANTYPE.Range;
		}
		if(SHC_GRAN.equals("singleton")){
			return GRANTYPE.Singleton;
		}
		return GRANTYPE.Singleton;
	}
	
	
	// used for other function 
	public void setTragetVal(String tval){
		
		switch(getGranType()){
			case List:
				tval=tval.replace("[", "(");
				tval=tval.replace("]", ")");
				break;
			case Range:
				tval=tval.replace("[", "");
				tval=tval.replace("]", "");
				
				String[] vals=tval.split(",");
				
				if(SHC_OUTPUT_TYPE.equals("float") || SHC_OUTPUT_TYPE.equals("int") ){
					// numerica cast to double to uniform processing 
					double[] result=new double[2];
					int count=0;
					for(String valString:vals){
						result[count]=Double.parseDouble(valString);
						count++;
					}
					
					double lower=Math.min(result[0],result[1]);
					double upper=Math.max(result[0],result[1]);
					//use between to format it 
					tval=String.format("between %.2f and %2.f", lower, upper);
				}else{
					// sort the list of string by alphabetical order
					List<String> valsList=(List) Arrays.asList(vals);
					Collections.sort(valsList);
					tval=String.format("between %s and %s ", valsList.get(0),valsList.get(1));
				}
				break;
			default:
				break;
		}
		// done 
		SHC_TargetVal=tval;
		
	}
	
	
	
	// 
	public void setSourceVal(String sval){
		SHC_SVal=sval;
//		// just for debugging 
//		if(SHC_GRAN.equals("singleton")){
////			SHC_TargetVal="4";
//			setTragetVal("4");
//		}
//		if(SHC_GRAN.equals("range")){
//			//SHC_TargetVal="[4, 5]";
//			setTragetVal("[4,5]");
//			
//		}
//		if(SHC_GRAN.equals("list")){
//			//SHC_TargetVal="[4,5]";
//			setTragetVal("[4,5]");
//		}
//		// done 
	}

	public String getSHC_SVAL(){

		return SHC_SVal;
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
