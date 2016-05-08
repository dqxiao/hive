package org.apache.hadoop.hive.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

public class VerifyQueryDriver {
	static List<String> parseResult=new ArrayList<String>();
	static String result="";
	
	static boolean DEBUG=true;
	
	static enum VQType{
		None, MapOnly, MapReduce;
	}
	
	static List<String> patterns=new ArrayList<String>();
	
	private static String matchPatternGen(String[] keywords, VQType type){
		StringBuilder sb=new StringBuilder();
		String sep="[ ]{1,}";
		String linesep="[ ]{0,}[\\n]{0,1}[ ]{0,}";
		boolean nextLine=false;
		VQType termType=type;
		for(int i=0;i<keywords.length;i++){
			if(i==0){
				sb.append(keywords[i]);
			}else{
				//nextLine=false;
				if(keywords[i].compareTo("nextLine")==0){
					nextLine=true;
					termType=type;
					continue;
				}
				if(keywords[i].compareTo("MapOnly")==0){
					termType=VQType.MapOnly;
					nextLine=true;
					continue;
				}
				if(keywords[i].compareTo("MapReduce")==0){
					termType=VQType.MapReduce;
					nextLine=true;
					continue;
				}
				//otherwise just skip it 
				if(termType==type){
					
					if(nextLine){
						sb.append(linesep);
						sb.append(keywords[i]);
						nextLine=false;
					}else{
						sb.append(sep);
						sb.append(keywords[i]);
					}
				}
				
				
			}
			
			
		}
		
		
		return sb.toString();
	}
	
	public static void matchPatternGen(){
		String[] keywords={
				"Verify","Correlation","(\\w+[,( \\w+)]*)", "nextLine",
				"Piggybacking", "(None|Map-only|MapReduce)","nextLine",
				"MapOnly", "mapOutputPercentage","(\\d+)" , "nextLine",
				"MapReduce", "IntermediateOutputPercentage", "(\\d+)","nextLine",
				"[;]{0,1}"
		};
		
		
		patterns.add(matchPatternGen(keywords,VQType.None));
		patterns.add(matchPatternGen(keywords,VQType.MapOnly));
		patterns.add(matchPatternGen(keywords,VQType.MapReduce));
		
		
	}
	
	
	private static int processCmd(String cmd, VQType type){
		parseResult.clear(); // default setting for this result 
		result="";
		
		String pString=patterns.get(type.ordinal());
	
		
		Pattern p=Pattern.compile(pString);
		Matcher m=p.matcher(cmd);
		
		
		
		if(m.matches()){
			for(int i=1;i<m.groupCount()+1;i++){
				parseResult.add(m.group(i));
				if(DEBUG){
					System.out.printf("presult:%s\n", m.group(i)); 
				}
			}
		}else{
			return -1;
		}
	
		if(type==VQType.MapOnly){
			
			String typeQuery=parseResult.get(1);
			typeQuery=typeQuery.toLowerCase();
			if(!typeQuery.equals("map-only")){
				parseResult.clear();
			}
		}
		
		if(type==VQType.MapReduce){
			String typeQuery=parseResult.get(1);
			typeQuery=typeQuery.toLowerCase();
			if(!typeQuery.equals("mapReduce")){
				parseResult.clear();
			}
		}
		
		
		if(!parseResult.isEmpty()){
			return type.ordinal();
		}
		return -1; // not process correctly; 
	}
	
	private static int processCmd(String cmd){
		matchPatternGen();
		
		
		
		for(VQType type: VQType.values()){
			int res=processCmd(cmd,type);
			if(res!=-1){
				return res;
			}
		}
		
		
		return -1;
	}
	
	
	public static void main(String[] args) {
//	    System.out.printf("Hello World \n");
////		String testCmd="Verify Correlation test, test1, test2 \n"+
////				"Piggybacking None;";
	    
//	    String testCmd="Verify Correlation test, test1, test2 \n"
//	    		+"Piggybacking MapReduce \n"
//	    		+"mapOutputPercentage 10 \n"
//	    		+";";
////	    
//			
//		int res=processCmd(testCmd);
//			
//		System.out.printf("res:%d\n",res);
	 }
}
