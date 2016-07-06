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



import org.ini4j.Ini;
import java.io.File;

/**
 *
 *  For maintain violation values fo multiple correlation 
 *
 */
public class MaintainQueryDriver {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	static enum VQType{
		None
	}
	

	private static List<String> parseResult=new ArrayList<String>();	
	private static boolean DEBUG=false;
	private static boolean LOG=true;
	private static VQType queryType=VQType.None; //default queryType 

	private static String metaQueryFormat="select *  from shcorrelation where shc_name in (%s)"; 

	private static Ini jobProp;

	
	static List<String> patterns=new ArrayList<String>();

	static List<Correlation> correlations=new ArrayList<Correlation>();



	
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
				"Maintain","Correlation","(\\w+[,( \\w+)]*)", "nextLine",
				"Piggybacking", "(None|MapOnly)","nextLine",
				"[;]{0,1}"
		};
		
		
		patterns.add(matchPatternGen(keywords,VQType.None));
		// patterns.add(matchPatternGen(keywords,VQType.MapOnly));
		// patterns.add(matchPatternGen(keywords,VQType.MapReduce));
			
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

		if(type==VQType.None){
			String typeQuery=parseResult.get(1);
			typeQuery=typeQuery.toLowerCase();
			if(!typeQuery.equals("none")){
				parseResult.clear();
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
	
	private static String metaUpdateCmdGen(String shc_name) {

		String updateFormat="update shcorrelation set SHC_STATUS=\'M\' where SHC_NAME=\'%s\' ";

		String result=String.format(updateFormat,shc_name);

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

           	// update 
           	for(Correlation cor: correlations ){
           		String shc_name =cor.SHC_NAME; 
           		String query=metaUpdateCmdGen(shc_name); 

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


	/**
	 *
	 * Gen Meta Query 
	 * Meta Query: correlation information via sql query 
	 *
	 */
	
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

	/**
	 *
	 * Get all info about correlation for maintaining 
	 *
	 */
	
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
           		System.out.printf("Metastore connection name: %s \n", conf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME));
           		System.out.printf("Metastore connection name : %s \n", conf.getVar(ConfVars.METASTORECONNECTURLKEY));
           	}

         
           	ResultSet rs = st.executeQuery(query);

           	System.out.printf("open metastore sucessful \n");
           

          	
          	// Hard code is typical wrong choice 

           	while(rs.next()){
           		Correlation cor=new Correlation(rs);

           		if(DEBUG){
           			System.out.printf("corerlation: %s\n", cor.toString()); 
           		}

           		if(cor.isSoft()&& cor.waitForMaintain()){
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

		sb.append("maintain.py ");

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
		String query="";
		if(queryType==VQType.None){
			sconf=verifyNoneConf(); 
		}



		if(queryType==VQType.None ){
			query=String.format(queryFormat, correlations.get(0).SHC_TNAME, sconf);
		}

		return query;
	}

	/**
	 *
	 * Create sep folder for holding maintained violated values 
	 *
	 */
	private static void scriptHDFSSetting(Statement stmt){
		String subDirFormat="dfs -mkdir /maintain/%s";

		for(Correlation cor: correlations){
			String subDirQuery=String.format(subDirFormat,cor.SHC_NAME);

			try{
				runHiveCmdUpdate(stmt,subDirQuery);
			}catch(Exception e){
				System.err.println("Caught: " + e.getMessage());
			}

		}

	}

	



	/*
	* loading necessary script in this session to distributed cache for running 
	* including: 
	* script for test correlation 
	* script for execution: verify, maponly_verify 
	*/
	private static void scriptLoading(Statement stmt){
		String homePath=System.getenv("HIVE_HOME");
		String scriptPath=homePath+"/userScripts/";
		String loadQueryFormat="add file %s%s%s";
		String type=".py";

		// loading test correlation script 
		for(Correlation cor:correlations){
			String loadquery=String.format(loadQueryFormat, scriptPath,cor.SHC_NAME, type);
			try{
				//runHiveCmdUpdate(loadquery);
				runHiveCmdUpdate(stmt,loadquery);

			}catch(Exception e){
				System.err.println("Caught: " + e.getMessage());
			}
		}

		// loading verify script for different purporse 
		if(queryType==VQType.None){
			String loadquery=String.format(loadQueryFormat,scriptPath,"maintain",type);

			try{
				runHiveCmdUpdate(stmt,loadquery);
			}catch(Exception e){
				System.err.println("Caught: " + e.getMessage());
			}
		}


	}



	private static void scriptRuning(Statement stmt) {

		if(queryType==VQType.None){
			String query=hiveCmdGen();
			try{
				runHiveCmdUpdate(stmt,query);
			}catch(Exception e){
				System.err.println("Script Running Caught: " + e.getMessage());
			}
		}

		

	}

	/**
	 *
	 * Store the violated tuple into temp_table 
	 * copy the schema of original table 
	 */
	private static void scriptMergeStore(Statement stmt){

		String mergeQueryFormat="CREATE TABLE  %s \n"
							+"like %s \n"
							+"ROW FORMAT DELIMITED \n"
							+"FIELDS TERMINATED BY \',\' \n"
							+"LINES TERMINATED BY \'\\n\' \n"
							+"location \'/maintain/%s/\'" ;




		for(Correlation cor:correlations){
			String shc_name=cor.SHC_NAME;
			String name=String.format("%s_%s", cor.SHC_TNAME, shc_name); 

			String output_type=cor.SHC_INPUT_TYPE; 

			String mergeQuery=String.format(mergeQueryFormat,name,cor.SHC_TNAME, shc_name);


			try{
				runHiveCmdUpdate(stmt, mergeQuery);
			}catch(Exception e){
				System.err.println("merge query caught: " + e.getMessage());
			}

			if(LOG){
				System.out.printf("store violated tuples of correlation %s in table %s \n", shc_name, name);
			}

			
			//done 

		}

	}



	/**
	 *
	 * Run Script for storing violated tuples in sep tables 
	 *
	 */
	
	

	private static void runMerge() throws Exception{
		Connection conn = null;
		try{
			conn= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
			Statement stmt = conn.createStatement();
			scriptMergeStore(stmt);

		}finally{
			if(conn!=null){
				conn.close();
			}
		}

	}

	/**
	 *
	 *	runMetaCmd: get info of correlations 
	 *	scriptLoading: loading correlation scripts and maintain scripts
	 *	scriptHDFSSetting: create folder for checking 
	 *	scriptRunning:	maintain ... 
	 */
	

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
				// better to read for configuration 
				conn= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
				Statement stmt = conn.createStatement();
	

				scriptLoading(stmt);
				scriptHDFSSetting(stmt);
				// helpScriptLoading(stmt); 
				scriptRuning(stmt);
			}
			finally {
	            if (conn != null) {
	                conn.close();
	            }
        	}

        	// done 

        	runMetaUpdateCmd();  
        	runMerge();
       	
       		// done 
		}else{
			System.out.printf("Maintain Correlations requires all correlations are defined over the same table \n"); 

		}


	}


	

	private static void runHiveCmdUpdate(Statement stmt,String query) throws Exception{
		if(LOG){
			System.out.printf("execute Query: \n %s\n",query);
		}
		stmt.execute(query);
	}

	


	
	public static void main(String[] args) {	
	 }
}
