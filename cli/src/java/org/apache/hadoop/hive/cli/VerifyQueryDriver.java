package org.apache.hadoop.hive.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;


public class VerifyQueryDriver {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	static enum VQType{
		None, MapOnly, MapReduce;
	}
	

	private static List<String> parseResult=new ArrayList<String>();	
	private static boolean DEBUG=false;
	private static boolean LOG=true;
	private static VQType queryType=VQType.None; //default queryType 

	private static String metaQueryFormat="select *  from shcorrelation where shc_name in (%s)"; 



	
	static List<String> patterns=new ArrayList<String>();

	static List<Correlation> correlations=new ArrayList<Correlation>();


	static HashMap<String,Integer> corNumMap=new HashMap<String, Integer>();
	
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
		patterns.clear();
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
			}else{
				System.out.printf("mapOutputPercentage clauase only available when Piggybacking= map-only \n");
			}
		}
		
		if(type==VQType.MapReduce){
			String typeQuery=parseResult.get(1);
			typeQuery=typeQuery.toLowerCase();
			if(!typeQuery.equals("mapReduce")){
				parseResult.clear();
			}else{
				System.out.printf("IntermediateOutputPercentage clauase only available when Piggybacking= map-reduce \n");
			}
		}
		
		
		if(!parseResult.isEmpty()){
			return type.ordinal();
		}
		return -1; // not process correctly; 
	}
	

	/**
	* refactor input command line and set queryType 
	*/
	public static int refactorCmd(String cmd){
		matchPatternGen();
		
		
		
		for(VQType type: VQType.values()){
			int res=processCmd(cmd,type);
			if(res!=-1){
				queryType=type; 
				return res;
			}
		}
		
		
		return -1;
	}
	
	private static String metaUpdateCmdGen(String shc_name, Integer shc_nv) {

		String updateFormat="update shcorrelation set SHC_STATUS=\'V\', SHC_NNUM_VIOLATION=%d where SHC_NAME=\'%s\' ";

		String result=String.format(updateFormat,shc_nv,shc_name);

		return result;
	}

	private static void runMetaUpdateCmd() throws Exception{
		Connection conn = null;
        try {

            HiveConf conf = new HiveConf();
           
           	String homePath=System.getenv("HIVE_HOME");
           	//System.out.printf("hive home setting:%s\n", homePath); 
            conf.addResource(new Path(homePath+"/conf/hive-site.xml"));
            Class.forName(conf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
            conn = DriverManager.getConnection(
                    conf.getVar(ConfVars.METASTORECONNECTURLKEY),
                    conf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
                    conf.getVar(ConfVars.METASTOREPWD));

            Statement st = conn.createStatement();
           	int row=0;
           	for(String shc_name: corNumMap.keySet()){
           		Integer shc_nv=corNumMap.get(shc_name);
           		String query=metaUpdateCmdGen(shc_name,shc_nv);

           		if(LOG){
           			System.out.printf("Metastore update query :%s \n",query);
           		}

           		int ret=st.executeUpdate(query);

           		row+=ret;
           	}
           	System.out.printf("Query OK, %d row in Metastore.sharc affected \n",row);

           	//
        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }
	}

	private static String metaQueryGen(){

		String corString=parseResult.get(0); 

		String[] corrs=corString.split(",");  // this format requirement 

		int size=corrs.length;

		// trim black space for further mathcing and add '' 
		StringBuilder sb=new StringBuilder();
		for(int i=0;i<size;i++){
			corrs[i]=corrs[i].trim();
		
			corrs[i]=String.format("\'%s\'", corrs[i]);

			if(i==0){
				sb.append(corrs[i]);
			}else{
				sb.append(",");
				sb.append(corrs[i]); 
			}


			if(DEBUG){
				System.out.printf("%d Verify Correlation: %s\n", i, corrs[i]);
			}
		}

		
		String result=String.format(metaQueryFormat,sb.toString());



		return result;
	

	}

	private static void runMetaCmd() throws Exception{
		correlations.clear(); // clear the previous setting
		Connection conn = null;
        try {

            HiveConf conf = new HiveConf();
           
           	String homePath=System.getenv("HIVE_HOME");
           	//System.out.printf("hive home setting:%s\n", homePath); 
            conf.addResource(new Path(homePath+"/conf/hive-site.xml"));
            Class.forName(conf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
            conn = DriverManager.getConnection(
                    conf.getVar(ConfVars.METASTORECONNECTURLKEY),
                    conf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
                    conf.getVar(ConfVars.METASTOREPWD));

            Statement st = conn.createStatement();
           	String query=metaQueryGen(); 
           	if(LOG){
           		System.out.printf("connect to metastore \n");
           		System.out.printf("Execute Query: %s \n",query);

           	}

           	ResultSet rs = st.executeQuery(query);
          	
          	// Hard code is typical wrong choice 

           	while(rs.next()){
           		Correlation cor=new Correlation(rs);

           		if(DEBUG){
           			System.out.printf("corerlation: %s\n", cor.toString()); 
           		}

           		if(cor.isSoft()){
           			correlations.add(cor);
           		}else{
           			if(LOG){
           				System.out.printf("skip hard correlation: %s \n", cor.SHC_NAME);  
           			}
           		}
           	}

        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }
	}


	private static boolean checkSameTable(){
		String tableName="";
		boolean flag=true;
		for(Correlation cor: correlations){
			if(tableName.equals("")){
				tableName=cor.SHC_TNAME;	
			}else{
				if(!tableName.equals(cor.SHC_TNAME)){
					flag=false;
					break;
				}
			}
		}

		return flag;
	}

	private static String verifyNoneConf(){
		StringBuilder sb =new StringBuilder();

		sb.append("verify.py ");

		for(Correlation cor: correlations){
			sb.append(cor.toQuery());
		}

		return sb.toString();

	}
	private static String hiveCmdGen(){
		String queryFormat="from %s \n"
			+"select transform(*) \n"
			+ "using \'python %s\' ";
			
		String sconf="";

		if(queryType==VQType.None){
			sconf=verifyNoneConf(); 
		}

		String query=String.format(queryFormat, correlations.get(0).SHC_TNAME, sconf);

		return query;
	}


	private static void scriptLoading(Statement stmt){
		String homePath=System.getenv("HIVE_HOME");
		String scriptPath=homePath+"/userScripts/";
		String loadQueryFormat="add file %s%s%s";
		String type=".py";

		for(Correlation cor:correlations){
			String loadquery=String.format(loadQueryFormat, scriptPath,cor.SHC_NAME, type);
			try{
				//runHiveCmdUpdate(loadquery);
				runHiveCmdUpdate(stmt,loadquery);

			}catch(Exception e){
				System.err.println("Caught: " + e.getMessage());
			}
		}

		String loadquery=String.format(loadQueryFormat,scriptPath,"verify",type);

		try{
			runHiveCmdUpdate(stmt,loadquery);
		}catch(Exception e){
			System.err.println("Caught: " + e.getMessage());
		}
	}

	private static void scriptRuning(Statement stmt) {
		String query=hiveCmdGen();

		try{
			runHiveCmdExecute(stmt,query);
		}catch(Exception e){
			System.err.println("Script Running Caught: " + e.getMessage());
		}
	}


	public static void run() throws Exception{
		runMetaCmd();// get the configuration for translation 
		boolean flag=checkSameTable();
		if(flag){

			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e){
				e.printStackTrace();
				return;
			}

			Connection conn = null;
			try{
				conn= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
				Statement stmt = conn.createStatement();
				scriptLoading(stmt);
				scriptRuning(stmt); // scriptLoading with problem 
			}
			finally {
	            if (conn != null) {
	                conn.close();
	            }
        	}

        	runMetaUpdateCmd();

		}else{
			System.out.printf("Verify Correlations requires all correlations are defined over the same table \n"); 

		}


	}

	private static void parseViolationResult(String valString){


		

		

		String[] vals=valString.split("\t");  
		Integer cval=Integer.parseInt(vals[1]);

		if(corNumMap.containsKey(vals[0])){
			Integer v_val=corNumMap.get(vals[0]);
			corNumMap.put(vals[0], v_val+cval);
		}else{
			corNumMap.put(vals[0], cval);
		}

		// 
		// if(v_val!=null){
		// 	
		// }else{
		// 	corNumMap.put(vals[0], cval);
		// }


	}

	private static void runHiveCmdExecute(Statement stmt,String query) throws Exception{
	
		if(LOG){
			System.out.printf("execute Query:%s\n",query);
		}
		ResultSet rs=stmt.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		// just print header 
		System.out.printf("%s\t%s \n","SHC_NAME","SHC_NNUM_VIOLATION");  

		

		while(rs.next()){
			StringBuilder sb=new StringBuilder();

			for (int i = 1; i <= columnsNumber; i++) {
		        // if (i > 1) System.out.print("\t");
		       	// 
		        // System.out.print(columnValue);
				String columnValue = rs.getString(i);
		        if(i==1){
		        	
		        	sb.append(columnValue);
		        }else{
		        	sb.append("\t");
		        	sb.append(columnValue);
		        }
		       
			}
			System.out.printf("%s\n",sb.toString());
			parseViolationResult(sb.toString());
		}

	}


	private static void runHiveCmdUpdate(Statement stmt,String query) throws Exception{
		if(LOG){
			System.out.printf("execute Query:%s\n",query);
		}
		stmt.execute(query);
	}

	


	
	public static void main(String[] args) {	
	 }
}
